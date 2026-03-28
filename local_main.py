import uuid
import requests
import json
import logging
from schema.register_client.registry_client import register_all_schemas
from producer.avro_producer import produce_records
from config.settings import TOPIC_SCHEMA_MAP



def get_random_user():
    try:
        response = requests.get('https://randomuser.me/api/')
        response.raise_for_status()
        data = response.json()
        return data['results'][0]

    except requests.exceptions.ConnectionError:
        print("Error: Failed to connect. Check your internet connection.")

    except requests.exceptions.Timeout:
        print("Error: The request timed out.")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.reason}")

    except requests.exceptions.RequestException as e:
        print(f"An unexpected requests error occurred: {e}")

    except ValueError:
        print("Error: Failed to parse JSON response.")


def format_data(response):
    data = {}

    # data['id'] = uuid.uuid4()
    data['id'] = str(uuid.uuid4())
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = str(location['postcode'])
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data



if __name__ == "__main__":

    # Get the Data from API

    data = get_random_user()
    user_data = format_data(data)

#     user_data = {
#     'id'             : uuid.UUID('77f6a4bb-81b5-4338-b9ab-fd262d240d9d'),
#     'first_name'     : 'Peter',
#     'last_name'      : 'Guttorm',
#     'gender'         : 'male',
#     'address'        : '5948 Henriks vei, Breivikbotn, Hedmark, Norway',
#     'post_code'      : '4003',
#     'email'          : 'peter.guttorm@example.com',
#     'username'       : 'tinybird462',
#     'dob'            : '1989-05-12T21:42:33.434Z',
#     'registered_date': '2017-11-06T08:42:55.570Z',
#     'phone'          : '25204203',
#     'picture'        : 'https://randomuser.me/api/portraits/med/men/35.jpg',
# }




    # Register a schema
    register_all_schemas()

    # produce the Data to topic
    topic = "random-users-info"

    report = produce_records(
        topic        = topic,
        records      = [user_data],
        key_field    = "id",
        skip_invalid = True,  
    )

