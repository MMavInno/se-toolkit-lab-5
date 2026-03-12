"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API."""
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )
            else:
                break

    return all_logs


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    from app.models.item import ItemRecord

    new_count = 0

    lab_title_map: dict[str, str] = {}
    for item in items:
        if item["type"] == "lab":
            lab_title_map[item["lab"]] = item["title"]

    lab_records: dict[str, ItemRecord] = {}
    for lab_short_id, lab_title in lab_title_map.items():
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title
            )
        )
        existing = result.first()

        if existing is None:
            new_lab = ItemRecord(type="lab", title=lab_title)
            session.add(new_lab)
            await session.flush()
            lab_records[lab_short_id] = new_lab
            new_count += 1
        else:
            lab_records[lab_short_id] = existing

    for item in items:
        if item["type"] != "task":
            continue

        task_title = item["title"]
        lab_short_id = item["lab"]
        lab_record = lab_records.get(lab_short_id)
        parent_id = lab_record.id if lab_record else None

        if parent_id is None:
            continue

        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_id
            )
        )
        existing = result.first()

        if existing is None:
            new_task = ItemRecord(type="task", title=task_title, parent_id=parent_id)
            session.add(new_task)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    new_count = 0

    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        key = (item["lab"], item.get("task"))
        item_title_lookup[key] = item["title"]

    for log in logs:
        lab_short_id = log["lab"]
        task_short_id = log.get("task")
        item_title = item_title_lookup.get((lab_short_id, task_short_id))

        if item_title is None:
            continue

        student_id = log["student_id"]
        result = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = result.first()

        if learner is None:
            learner = Learner(
                external_id=student_id,
                student_group=log.get("group", ""),
            )
            session.add(learner)
            await session.flush()

        result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = result.first()

        if item is None:
            continue

        result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        existing_interaction = result.first()

        if existing_interaction is not None:
            continue

        submitted_at_str = log["submitted_at"]
        submitted_at = datetime.fromisoformat(
            submitted_at_str.replace("Z", "+00:00")
        )

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=submitted_at,
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    from app.models.interaction import InteractionLog

    items = await fetch_items()
    await load_items(items, session)

    result = await session.exec(
        select(InteractionLog).order_by(InteractionLog.created_at.desc())
    )
    last_interaction = result.first()

    since = last_interaction.created_at if last_interaction else None

    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    result = await session.exec(
        select(func.count()).select_from(InteractionLog)
    )
    total_records = result.one()

    return {"new_records": new_records, "total_records": total_records}
