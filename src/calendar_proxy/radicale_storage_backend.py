import runpy
import contextlib
import datetime
import logging
import urllib
import urllib.parse
from collections.abc import Generator
from typing import Optional, Callable, ContextManager, Iterable, Mapping

import ical.event
import ical.calendar_stream
import ical.calendar

import radicale.item
import radicale.storage.multifilesystem_nolock
import radicale.types

from . import nodes

logger = logging.getLogger("my_proxy_logger")


CALENDARS = runpy.run_path("./config/calendar_config.py")["get_calendars"]()


# class Storage(radicale.storage.multifilesystem_nolock.Storage):
#     def discover(
#         self, path: str, depth: str = "0",
#         child_context_manager: Optional[
#             Callable[[str, Optional[str]], ContextManager[None]]] = None,
#         user_groups: Set[str] = set([])
#     ) -> Iterator[types.CollectionOrItem]:
#         out = list(super().discover(path, depth, child_context_manager, user_groups))
#         logger.info(
#             f"Discovering path: {path}, depth: {depth}, user_groups: {user_groups}\n"
#             f"{[f"I: {c.href}" if isinstance(c, radicale.item.Item) else f"C: {c.path}" for c in out]}"
#         )
#         return iter(out)


def convert_event(event: ical.event.Event, collection: radicale.storage.BaseCollection) -> radicale.item.Item:
    # component = event.__encode_component_root__()
    # component.name = "VEVENT"
    # vevent_ics = component.ics().replace("\r\n", "\n").replace("\n", "\r\n")
    #
    # full_ics = (
    #     f"BEGIN:VCALENDAR\r\n"
    #     "VERSION:2.0\r\n"
    #     "CALSCALE:GREGORIAN\r\n"
    #     "PRODID:-//Ximian//NONSGML Evolution Calendar//EN\r\n"
    #     f"{vevent_ics}\r\n"
    #     "END:VCALENDAR\r\n"
    # )

    # logger.info("CP GET: %s", full_ics)

    return radicale.item.Item(
        text=ical.calendar_stream.IcsCalendarStream.calendar_to_ics(ical.calendar.Calendar(events=[event])),
        # text=full_ics,
        collection=collection,
        # href=f"{collection.path}/{urllib.parse.quote(event.uid, safe='')}.ics",
        href=CalendarProxyCollection.uid_to_href(event.uid),
        # name="VCALENDAR",
        # component_name="VEVENT"
    )


class CalendarProxyCollection(radicale.storage.BaseCollection):
    """
    Represents a specific calendar. Radicale calls these methods when a
    client asks for calendar data or attempts to change it.
    """

    def __init__(self, path: str, props: dict[str, str], node: nodes.Node):
        self._path = path
        self._props = props
        self._node = node

    @property
    def path(self) -> str:
        """The sanitized path of the collection without leading or
        trailing ``/``."""
        return self._path

    @staticmethod
    def href_to_uid(href: str) -> str:
        return urllib.parse.unquote(href.rsplit(".")[0])

    @staticmethod
    def uid_to_href(uid: str) -> str:
        return f"{urllib.parse.quote(uid, safe='')}.ics"

    def get_multi(self, hrefs: Iterable[str]) -> Iterable[tuple[str, Optional[radicale.item.Item]]]:
        """Fetch multiple items.

        It's not required to return the requested items in the correct order.
        Duplicated hrefs can be ignored.

        Returns tuples with the href and the item or None if the item doesn't
        exist.
        """

        events = self._node.get()

        for href in hrefs:
            uid = self.href_to_uid(href)

            if uid in events:
                yield href, convert_event(events[uid], self)
            else:
                yield href, None

    def get_all(self) -> Iterable[radicale.item.Item]:
        """Fetch all items."""
        """
        PROXY: Triggered when the client syncs the calendar.
        Fetch the remote ICS file, split it into individual events, and yield them.
        """
        logger.info(f"Sync requested for path: {self.path}")

        for event in self._node.get().values():
            yield convert_event(event, self)

    def upload(self, href: str, item: radicale.item.Item) -> tuple[radicale.item.Item, Optional[radicale.item.Item]]:
        """Upload a new or replace an existing item.

        Return the uploaded item and the old item if it was replaced.
        """
        """
        HOOK: Triggered when the client creates (PUT) or modifies an event.
        cal_item.text contains the raw iCalendar string pushed by the client.
        """

        (_, old_item), = self.get_multi((href,))

        updated_items = ical.calendar_stream.IcsCalendarStream.calendar_from_ics(item.serialize()).events

        # TODO: Handle events with the same UID but different sequence numbers. For now, we just pick the event with the highest sequence number.
        most_recent_event = max(updated_items, key=lambda e: e.sequence or 0)

        self._node.backpropagate([nodes.EventAction(most_recent_event, "add_or_change")])

        return item, old_item

    def delete(self, href: Optional[str] = None) -> None:
        """Delete an item.

        When ``href`` is ``None``, delete the collection.

        """

        if href is None:
            raise RuntimeError("Cannot delete Collection")

        uid = self.href_to_uid(href)

        events = self._node.get()

        self._node.backpropagate([nodes.EventAction(events[uid], "delete")])

    def get_meta(self, key: Optional[str] = None) -> Mapping[str, str] | Optional[str]:
        """Get metadata value for collection.

        Return the value of the property ``key``. If ``key`` is ``None`` return
        a dict with all properties

        """
        """Provide metadata about the calendar to the client."""

        if key is None:
            return self._props
        # logger.info(f"Getting metadata for key: {key}")
        return self._props.get(key)

    def set_meta(self, props: Mapping[str, str]) -> None:
        """Set metadata values for collection.

        ``props`` a dict with values for properties.

        """
        raise NotImplementedError(f"Can't set Props of Collection {props}!")

    @property
    def last_modified(self) -> str:
        """Get the HTTP-datetime of when the collection was modified."""
        return datetime.datetime.now(tz=datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        # raise NotImplementedError


class FolderProxyCollection(radicale.storage.BaseCollection):
    def __init__(self, path: str):
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    def get_meta(self, key: Optional[str] = None) -> Mapping[str, str] | Optional[str]:
        if key is None:
            return {}
        return None


class Storage(radicale.storage.BaseStorage):
    def discover(
        self, path: str, depth: str = "0",
        child_context_manager: Optional[Callable[[str, Optional[str]], ContextManager[None]]] = None,
        user_groups: set[str] = set()
    ) -> Iterable[radicale.types.CollectionOrItem]:
        """Discover a list of collections under the given ``path``.

        ``path`` is sanitized.

        If ``depth`` is "0", only the actual object under ``path`` is
        returned.

        If ``depth`` is anything but "0", it is considered as "1" and direct
        children are included in the result.

        The root collection "/" must always exist.
        """

        p = {
            # "C:supported-calendar-component-set": "VEVENT",
            # "D:displayname": "Title1",
            # "ICAL:calendar-color": "#074440ff",
            "tag": "VCALENDAR"
        }

        path = path.lstrip("/").rstrip("/")

        if path == "" and depth == "0":
            yield FolderProxyCollection("")
            return

        if depth == "0":
            if path in CALENDARS:
                yield CalendarProxyCollection(path, props=p, node=CALENDARS[path])
            else:
                *calendar_components, href = path.split("/")

                calendar_path = "/".join(calendar_components)
                if calendar_path in CALENDARS:
                    # path references an Item
                    for href, event in CalendarProxyCollection(
                        calendar_path,
                        props=p, node=CALENDARS[calendar_path]
                    ).get_multi((href,)):
                        if event is not None:
                            yield event
                        else:
                            logger.error("Could not discover item %r in %r", href, path)
                else:
                    yield FolderProxyCollection(path)

            return

        if path not in CALENDARS:
            yield FolderProxyCollection(path)

            for calendar_path, calendar in CALENDARS.items():
                if calendar_path.startswith(path):
                    yield CalendarProxyCollection(calendar_path, props=p, node=calendar)

            return

        collection = CalendarProxyCollection(path, props=p, node=CALENDARS[path])

        yield collection
        yield from collection.get_all()

        return

    def move(self, item: radicale.item.Item, to_collection: radicale.storage.BaseCollection, to_href: str) -> None:
        """Move an object.

        ``item`` is the item to move.

        ``to_collection`` is the target collection.

        ``to_href`` is the target name in ``to_collection``. An item with the
        same name might already exist.

        """

        raise NotImplementedError("Moving a Collections / Calendars is not supported.")

    def create_collection(
        self, href: str,
        items: Optional[Iterable[radicale.item.Item]] = None,
        props: Optional[Mapping[str, str]] = None
    ) -> tuple[radicale.storage.BaseCollection, dict[str, radicale.item.Item], list[str]]:
        """Create a collection.

        ``href`` is the sanitized path.

        If the collection already exists and neither ``collection`` nor
        ``props`` are set, this method shouldn't do anything. Otherwise the
        existing collection must be replaced.

        ``collection`` is a list of vobject components.

        ``props`` are metadata values for the collection.

        ``props["tag"]`` is the type of collection (VCALENDAR or VADDRESSBOOK).
        If the key ``tag`` is missing, ``items`` is ignored.

        """

        raise NotImplementedError

    @contextlib.contextmanager
    def acquire_lock(self, mode: str, user: str = "", *args, **kwargs) -> Generator[None]:
        """Set a context manager to lock the whole storage.

        ``mode`` must either be "r" for shared access or "w" for exclusive
        access.

        ``user`` is the name of the logged in user or empty.

        """
        yield

    def verify(self) -> bool:
        return True


# class Storage(A):
#     def discover(
#         self, path: str, depth: str = "0",
#         child_context_manager: Optional[
#             Callable[[str, Optional[str]], ContextManager[None]]] = None,
#         user_groups: Set[str] = set([])
#     ) -> Iterator[types.CollectionOrItem]:
#         out = list(super().discover(path, depth, child_context_manager, user_groups))
#         logger.info(
#             f"Discovering path: {path}, depth: {depth}, user_groups: {user_groups}\n"
#             f"{[f"I: {c.href}" if isinstance(c, radicale.item.Item) else f"C: {c.path}" for c in out]}"
#         )
#         return iter(out)


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)8s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S", force=True)
