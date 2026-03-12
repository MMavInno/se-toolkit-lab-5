"""Router for analytics endpoints."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    lab_title_part = lab.replace("lab-", "Lab ").split(" — ")[0]

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title_part}%")
        )
    )
    lab_item = result.first()

    if lab_item is None:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_items = result.all()

    task_ids = [task.id for task in task_items]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
        else_="0-25"
    ).label("bucket")

    query = (
        select(bucket_case, func.count().label("count"))
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(bucket_case)
    )

    result = await session.exec(query)

    bucket_counts: dict[str, int] = {}
    for row in result:
        bucket_counts[row.bucket] = row.count

    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_title_part = lab.replace("lab-", "Lab ").split(" — ")[0]

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title_part}%")
        )
    )
    lab_item = result.first()

    if lab_item is None:
        return []

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        ).order_by(ItemRecord.title)
    )
    task_items = result.all()

    result_list = []
    for task in task_items:
        query = (
            select(
                func.avg(InteractionLog.score).label("avg_score"),
                func.count().label("attempts")
            )
            .where(InteractionLog.item_id == task.id)
            .where(InteractionLog.score.isnot(None))
        )

        result = await session.exec(query)
        row = result.first()

        if row and row.attempts > 0:
            result_list.append({
                "task": task.title,
                "avg_score": round(row.avg_score, 1) if row.avg_score else 0.0,
                "attempts": row.attempts,
            })

    return result_list


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    lab_title_part = lab.replace("lab-", "Lab ").split(" — ")[0]

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title_part}%")
        )
    )
    lab_item = result.first()

    if lab_item is None:
        return []

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_items = result.all()

    task_ids = [task.id for task in task_items]

    if not task_ids:
        return []

    query = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    result = await session.exec(query)

    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in result
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    lab_title_part = lab.replace("lab-", "Lab ").split(" — ")[0]

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title_part}%")
        )
    )
    lab_item = result.first()

    if lab_item is None:
        return []

    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_items = result.all()

    task_ids = [task.id for task in task_items]

    if not task_ids:
        return []

    query = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(query)

    return [
        {
            "group": row.group,
            "avg_score": round(row.avg_score, 1) if row.avg_score else 0.0,
            "students": row.students,
        }
        for row in result
    ]
