from flask import Blueprint, request, jsonify, g

trip_api = Blueprint('trip_api', __name__)


@trip_api.route('/api/trips', methods=['GET'])
def get_trips():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    hour_of_day = request.args.get('hour_of_day', type=int)
    day_of_week = request.args.get('day_of_week', type=int)
    is_weekend = request.args.get('is_weekend', type=bool)
    distance_category = request.args.get('distance_category')
    min_speed = request.args.get('min_speed', type=float)
    max_speed = request.args.get('max_speed', type=float)
    passenger_count = request.args.get('passenger_count', type=int)
    limit = request.args.get('limit', default=100, type=int)
    offset = request.args.get('offset', default=0, type=int)

    # Fetch data from the database via class method
    trips = g.db.get_trip_data(
        start_date=start_date,
        end_date=end_date,
        hour_of_day=hour_of_day,
        day_of_week=day_of_week,
        is_weekend=is_weekend,
        distance_category=distance_category,
        min_speed=min_speed,
        max_speed=max_speed,
        passenger_count=passenger_count,
        limit=limit,
        offset=offset,
    )

    return jsonify(trips)


@trip_api.route('/api/trips/statistics', methods=['GET'])
def trips_statistics():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    group_by = request.args.get('group_by')
    metrics = request.args.getlist('metrics')

    # Fetch statistics from the database via class method
    statistics = g.db.get_trip_statistics(
        start_date=start_date,
        end_date=end_date,
        group_by=group_by,
        metrics=metrics,
    )

    return jsonify(statistics)