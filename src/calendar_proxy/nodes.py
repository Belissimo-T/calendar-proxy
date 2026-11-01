import ical.compat
import random
import abc
import copy
import dataclasses
import datetime
import typing
from collections import defaultdict

from ical.calendar import Event, Calendar
import ical.calendar
from ical.calendar_stream import IcsCalendarStream
import ical.compat.timezone_compat
from upath import UPath
import caldav.collection
import caldav.calendarobjectresource
import caldav.lib.error

import pipifax_io.serializable


class CalendarProxyError(Exception): pass


@dataclasses.dataclass
class EventAction:
    event: Event
    type_: typing.Literal["add_or_change", "delete"]


Events = dict[str, Event]


class Node:
    def get(self) -> Events:
        raise CalendarProxyError(f"{self.__class__.__name__} cannot retrieve Events.")

    def backpropagate(self, data: list[EventAction]) -> None:
        self._cache_timestamp = None
        raise CalendarProxyError(f"{self.__class__.__name__} cannot process Event updates.")


@dataclasses.dataclass
class MemoryCacheNode(Node):
    source: Node
    max_cache_age: datetime.timedelta = datetime.timedelta(seconds=10)

    _cache_timestamp: datetime.datetime | None = dataclasses.field(init=False, default=None)
    _cache_data: Events | None = dataclasses.field(init=False, default=None)

    def get(self) -> Events:
        if self._cache_timestamp is None or (datetime.datetime.now() - self._cache_timestamp) > self.max_cache_age:
            self._cache_data = self.source.get()
            self._cache_timestamp = datetime.datetime.now()

        return self._cache_data.copy()

    def backpropagate(self, data: list[EventAction]) -> None:
        self._cache_timestamp = None
        self.source.backpropagate(data)


@dataclasses.dataclass
class BasicFilterNode(Node, abc.ABC):
    source: Node

    def backpropagate(self, data: list[EventAction]):
        self.source.backpropagate(data)


@dataclasses.dataclass
class MapEventsNode(BasicFilterNode):
    do: typing.Callable[[Event], Event]
    undo: typing.Callable[[Event], Event]

    def get(self) -> Events:
        return {(new_event := self.do(e)).uid: new_event for k, e in self.source.get().items()}

    def backpropagate(self, data: list[EventAction]):
        self.source.backpropagate([
            copy.replace(event_action, event=self.undo(event_action.event))
            for event_action in data
        ])


@dataclasses.dataclass
class IcalNode(Node):
    path: UPath
    not_found_ok: bool = False
    preprocessor: typing.Callable[[str], str] | None = None

    def _get_calendar(self) -> Calendar:
        preprocessor = self.preprocessor if self.preprocessor is not None else lambda x: x

        try:
            ics_content = self.path.read_text()
        except FileNotFoundError:
            if not self.not_found_ok:
                raise
            else:
                return Calendar()
        else:
            ics_content = preprocessor(ics_content)

            try:
                with ical.compat.enable_compat_mode(ics=ics_content):
                    return IcsCalendarStream.calendar_from_ics(ics_content)
            except Exception as e:
                e.add_note(f"Failed to parse calendar from {self.path!r}.")
                raise

    def get(self) -> Events:
        return {e.uid: e for e in self._get_calendar().events}

    def backpropagate(self, data: list[EventAction]):
        current_calendar = self._get_calendar()
        current_events = {e.uid: e for e in current_calendar.events}

        for event_action in data:
            if event_action.type_ == "add_or_change":
                current_events |= {event_action.event.uid: event_action.event}
            elif event_action.type_ == "delete":
                del current_events[event_action.event.uid]
            else:
                assert False

        updated_calendar = copy.replace(current_calendar, events=list(current_events.values()))

        try:
            self.path.write_text(IcsCalendarStream.calendar_to_ics(updated_calendar))
        except Exception as e:
            e.add_note(f"Failed to write updated calendar to {self.path!r}.")
            raise


@dataclasses.dataclass
class CalDavNode(Node):
    calendar: caldav.collection.Calendar

    def get(self) -> Events:
        events: Events = {}
        for caldav_event in typing.cast(list[caldav.calendarobjectresource.Event], self.calendar.get_events()):
            if caldav_event.data is None:
                raise RuntimeError(f"CalDAV event {caldav_event} has no data.")

            with (ical.compat.enable_compat_mode(
                ics=caldav_event.data), ical.compat.timezone_compat.enable_allow_invalid_timezones(), ical
                .compat.timezone_compat.enable_extended_timezones()):
                cal = IcsCalendarStream.calendar_from_ics(caldav_event.data)

            events |= {e.uid: e for e in cal.events}

        return events

    def backpropagate(self, data: list[EventAction]):
        for event_action in data:
            ics_data = IcsCalendarStream.calendar_to_ics(Calendar(events=[event_action.event]))
            try:
                event = typing.cast(
                    caldav.calendarobjectresource.CalendarObjectResource,
                    self.calendar.get_event_by_uid(event_action.event.uid)
                )
            except caldav.lib.error.NotFoundError:
                self.calendar.add_event(
                    ical=ics_data,
                    increase_seqno=False
                )
            else:
                event.data = ics_data
                event.save()


@dataclasses.dataclass
class MergeNode(Node):
    sources: typing.Sequence[
        tuple[Node, typing.Callable[[EventAction], bool]]
    ]

    # deduplicate: bool = True

    def get(self) -> Events:
        if not self.sources:
            return Events()

        events = self.sources[0][0].get()

        for source, _ in self.sources[1:]:
            events |= source.get()

        return events

    def backpropagate(self, data: list[EventAction]):
        updates: dict[int, list[EventAction]] = defaultdict(list)

        for event_action in data:
            for i, (node, condition) in enumerate(self.sources):
                if condition(event_action):
                    updates[i].append(event_action)

        for i, actions in updates.items():
            self.sources[i][0].backpropagate(actions)


class SimpleMergeNode(MergeNode):
    def __init__(self, sources: typing.Sequence[Node]):
        def make_event_condition(i: int) -> typing.Callable[[EventAction], bool]:
            def condition(event_action: EventAction) -> bool:
                event_data = EventData.from_event(event_action.event)
                try:
                    return event_data.peek_key_value_pair(f"{self.__class__.__name__}.source_index") == str(i)
                except KeyError:
                    return False

            return condition

        super().__init__(
            sources=[
                (RandomizeUidNode(
                    PushEventDataNode(s, key=lambda _: f"{self.__class__.__name__}.source_index",
                                      data=lambda _: str(i)),
                    seed=str(i)
                ), make_event_condition(i))
                for i, s in enumerate(sources)
            ]
        )


@dataclasses.dataclass
class PushEventDataNode(MapEventsNode):
    def __init__(self, source: Node, key: typing.Callable[[Event], str], data: typing.Callable[[Event], str]):
        super().__init__(
            source=source,
            do=lambda event: self._push_data(event),
            undo=lambda event: self._pop_data(event)
        )

        self._key = key
        self._data = data

    def _push_data(self, event: Event) -> Event:
        event_data = EventData.from_event(event)
        event_data.push_key_value_pair(self._key(event), self._data(event))
        return event_data.store_in_event(event)

    def _pop_data(self, event: Event) -> Event:
        event_data = EventData.from_event(event)
        event_data.pop_key_value_pair(self._key(event))
        return event_data.store_in_event(event)


CP_ESCAPE = "__cp"


@dataclasses.dataclass
class ApplyPatchesNode(Node):
    source: Node
    patches_source: Node

    def get(self) -> Events:
        events = self.source.get()

        for patch in self.patches_source.get().values():
            if self.does_event_mark_deletion(patch):
                del events[patch.uid]
            else:
                events[patch.uid] = patch

        return events

    @staticmethod
    def does_event_mark_deletion(event: Event) -> bool:
        return event.summary is not None and event.summary.startswith(f"[{CP_ESCAPE}.Deleted]")

    @staticmethod
    def make_event_mark_deletion(event: Event) -> Event:
        return copy.replace(event, summary=f"[{CP_ESCAPE}.Deleted]{event.summary}")

    def backpropagate(self, data: list[EventAction]):
        out = []

        for eventevent in data:
            if eventevent.type_ == "delete":
                out.append(EventAction(self.make_event_mark_deletion(eventevent.event), "add_or_change"))
            elif eventevent.type_ == "add_or_change":
                out.append(EventAction(eventevent.event, "add_or_change"))
            else:
                assert False

        self.patches_source.backpropagate(out)


@dataclasses.dataclass
class RedirectUpdatesNode(Node):
    source: Node
    updates_to: list[Node]

    def get(self):
        return self.source.get()

    def backpropagate(self, data: list[EventAction]):
        for update_node in self.updates_to:
            update_node.backpropagate(data)


@dataclasses.dataclass
class AddPrefixNode(BasicFilterNode):
    prefix: str

    def __post_init__(self):
        self._map_node = MapEventsNode(
            source=self.source,
            do=lambda event: copy.replace(
                event,
                summary=self.prefix + option_to_str(event.summary)),
            undo=lambda event: copy.replace(
                event,
                summary=(lambda x: x[len(self.prefix):] if x is not None else x)(str_to_option(event.summary))
            )
        )

    def get(self) -> Events:
        return self._map_node.get()

    def backpropagate(self, data: list[EventAction]):
        self._map_node.backpropagate(data)


@dataclasses.dataclass
class RandomizeUidNode(BasicFilterNode):
    seed: str
    _random: random.Random = dataclasses.field(init=False, repr=False, default_factory=random.Random)

    def do(self, event: Event) -> Event:
        random_data = self._random.randbytes(16).hex()
        event_data = EventData.from_event(event)
        event_data.push_key_value_pair(f"{self.__class__.__name__}.old_uid", event.uid)
        event = event_data.store_in_event(event)
        return copy.replace(event, uid=random_data)

    def undo(self, event: Event) -> Event:
        event_data = EventData.from_event(event)
        try:
            old_uid = event_data.pop_key_value_pair(f"{self.__class__.__name__}.old_uid")
        except KeyError:
            # event was added
            return event
        else:
            event = event_data.store_in_event(event)
            return copy.replace(event, uid=old_uid)

    def get(self) -> Events:
        self._random.seed(self.seed)
        return dict(
            ((new_event := self.do(e)).uid, new_event)
            for k, e in sorted(self.source.get().items(), key=lambda x: x[0])
        )

    def backpropagate(self, data: list[EventAction]):
        self.source.backpropagate([
            copy.replace(event_action, event=self.undo(event_action.event))
            for event_action in data
        ])


def option_to_str(x: str | None) -> str:
    return x or f"{CP_ESCAPE}.None"


def str_to_option(x: str | None) -> str | None:
    if x == f"{CP_ESCAPE}.None" or x is None:
        return None
    else:
        return x


EVENT_STORE_SENTINEL = f"\n--- {CP_ESCAPE}.EventData ---\n"


def update_data_in_event(event: Event, callback: typing.Callable[[str | None], str | None]) -> tuple[
    Event, str | None]:
    # if event.description is None:
    #     desc_before = data = None
    # elif EVENT_STORE_SENTINEL in event.description:
    #     desc_before, data = event.description.split(EVENT_STORE_SENTINEL)
    # else:
    #     desc_before, data = "", None
    #
    # data = callback(data)
    #
    # return copy.replace(
    #     event,
    #     description=f"{option_to_str(desc_before)}{EVENT_STORE_SENTINEL}{data}" if data is not None else str_to_option(
    #         desc_before)
    # ), data
    event = copy.copy(event)

    for extra in event.extras:
        if extra.name == "X-CALENDER-PROXY":
            data = extra.value
            break
    else:
        data = None

    event.extras = [extra for extra in event.extras if extra.name != "X-CALENDER-PROXY"]

    data = callback(data)

    if data is not None:
        event.extras.append(ical.calendar.ExtraProperty(
            name="X-CALENDER-PROXY",
            value=data
        ))

    return event, data


@dataclasses.dataclass
class EventData(pipifax_io.serializable.SimpleSerializable):
    store: list[tuple[str, str]] = dataclasses.field(default_factory=list)

    def is_empty(self) -> bool:
        return len(self.store) == 0

    # def key_value_store_contains_key(self, key: str) -> bool:
    #     for k, v in self.store:
    #         if k == key:
    #             return True
    #
    #     return False

    def peek_key_value_pair(self, key: str) -> str:
        for k, v in self.store:
            if k == key:
                return v

        raise KeyError(f"Key {key} not found in EventData.")

    def pop_key_value_pair(self, key: str) -> str:
        for i, (k, v) in enumerate(self.store):
            if k == key:
                del self.store[i]
                return v

        raise KeyError(f"Key {key} not found in EventData.")

    def push_key_value_pair(self, key: str, value: str) -> None:
        self.store.append((key, value))

    @classmethod
    def from_event(cls, event: Event):
        _, data = update_data_in_event(event, lambda x: x)

        if data is None:
            return cls()

        return cls.deserialize(data.encode())

    def store_in_event(self, event: Event) -> Event:
        event, _ = update_data_in_event(
            event,
            lambda x: self.serialize().decode() if not self.is_empty() else None
        )

        return event
