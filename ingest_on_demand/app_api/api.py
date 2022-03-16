from flask import Flask
from flask_restful import Resource, Api, reqparse

import helper
import sql_queries

app = Flask(__name__)
api = Api(app)


class Regions(Resource):
    def get(self):
        data = helper.pg_get_data(query=sql_queries.regions)
        return data, 200


class DatSources(Resource):
    def get(self):
        data = helper.pg_get_data(query=sql_queries.datasources)
        return data, 200


class LastLogEvent(Resource):
    def get(self):
        data = helper.pg_get_data(query=sql_queries.last_log_event)
        return data, 200


class CheckPosition(Resource):
    def post(self):
        parser = reqparse.RequestParser()

        parser.add_argument("latitude", required=True)
        parser.add_argument("longitude", required=True)
        args = parser.parse_args()

        # sample: {"latitude": 7.662608916626361, "longitude":45.07378523979249}
        data = helper.pg_get_data(
            query=sql_queries.get_trips_base_on_point, params=args
        )

        return data, 200


class Locations(Resource):
    # methods go here
    pass


# entry points
api.add_resource(Regions, "/regions")
api.add_resource(DatSources, "/datasources")
api.add_resource(CheckPosition, "/check_position")
api.add_resource(LastLogEvent, "/last_log_event")

if __name__ == "__main__":
    app.run()  # run our Flask app
