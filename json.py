from json_logic import jsonLogic

rule = {
    "if": [
        { "var": "data.insight_data.kpi.id" },
        { "kpi_id": { "var": "data.insight_data.kpi.id" } },
        { "error": "Missing key: data.insight_data.kpi.id" }
    ]
}

data_rq_example = {
    "data": {
        "insight_data": {
            "kpi": {
                "id": 1234
            }
        }
    }
}

result = jsonLogic(rule, data_rq_example)
print(result)  # Output: {'kpi_id': 1234}
