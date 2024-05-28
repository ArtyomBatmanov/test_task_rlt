import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pymongo
from pymongo.command_cursor import CommandCursor

from config import DATABASE, FORMAT_INTERVAL, MONGO_DB


class QueryParameters:
    def __init__(self, dt_from, dt_upto, group_type):
        self.dt_from = datetime.fromisoformat(dt_from)
        self.dt_upto = datetime.fromisoformat(dt_upto)
        self.group_type = group_type


async def create_date_list(start, end, interval_type):
    dates = []
    current = start
    interval_delta = {f"{interval_type}s": +1}
    while current <= end:
        dates.append(current)
        current += relativedelta(**interval_delta)
    return dates


async def build_response_data(
    query_results: CommandCursor, params: QueryParameters
) -> dict:
    data_points = []
    date_labels = []
    current_record = query_results.try_next()
    date_range = await create_date_list(
        params.dt_from, params.dt_upto, params.group_type
    )
    for point in date_range:
        formatted_date = point.strftime(FORMAT_INTERVAL[params.group_type])
        date_labels.append(formatted_date)
        salary_sum = 0
        if current_record and current_record["_id"] == formatted_date:
            salary_sum = current_record["total_salary"]
            current_record = query_results.try_next()
        data_points.append(int(salary_sum))
    return dict(dataset=data_points, labels=date_labels)


async def fetch_query_results(params: QueryParameters) -> CommandCursor:
    query_pipeline = [
        {"$match": {"dt": {"$gte": params.dt_from, "$lte": params.dt_upto}}},
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": FORMAT_INTERVAL.get(params.group_type, "month"),
                        "date": "$dt",
                    }
                },
                "total_salary": {"$sum": "$value"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    with pymongo.MongoClient(MONGO_DB) as client:
        db = client[DATABASE]
        query_results = db.salary.aggregate(query_pipeline)
    return query_results


async def retrieve_aggregate_data(request_data: dict) -> dict:
    params = QueryParameters(**request_data)
    query_results = await fetch_query_results(params)
    response_data = await build_response_data(query_results, params)
    return response_data


async def create_response(request_text: str) -> str:
    try:
        request_data = json.loads(request_text)
    except (SyntaxError, ValueError):
        return "Invalid data format"
    result = await retrieve_aggregate_data(request_data)
    return str(result).replace("'", '"')
