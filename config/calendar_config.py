import datetime

import caldav
import caldav.davclient
from upath import UPath

from calendar_proxy import nodes


def get_calendars():
    ccc_events_source = nodes.MemoryCacheNode(nodes.IcalNode(
        path=UPath(
            "https://c3voc.de/eventkalender/events.ical"
        ),
        preprocessor=lambda x: x.replace("BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nEND:VTIMEZONE", "")
    ), max_cache_age=datetime.timedelta(minutes=5))

    out = {
        "[CCC] Events": ccc_events_source,
    }

    out = {
        f"gemuesekohl/{calendar_name}": nodes.RandomizeUidNode(nodes.MemoryCacheNode(node), calendar_name)
        for calendar_name, node in out.items()
    }
    return out


if __name__ == "__main__":
    get_calendars()
