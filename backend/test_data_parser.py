from backend.agents.simple.data_parser import DataParserAgent
import json


def test_basic_parse():
    config = {
        "extract_fields": ["name", "age"],
        "remove_nulls": True,
    }
    agent = DataParserAgent(config=config)
    agent.validate()

    inputs = {
        "name": "Hassan",
        "age": 99,
        "extra": "ignored",
    }

    result = agent.run(inputs)
    print("Result dict input:", result)


def test_json_string_input():
    config = {
        "extract_fields": ["ht", "tva", "ttc"],
        "remove_nulls": True,
    }
    agent = DataParserAgent(config=config)
    agent.validate()

    json_input = json.dumps({
        "ht": 100,
        "tva": 20,
        "ttc": 120,
        "other": "ignored",
    })

    result = agent.run(json_input)
    print("Result JSON input:", result)


if __name__ == "__main__":
    test_basic_parse()
    test_json_string_input()
