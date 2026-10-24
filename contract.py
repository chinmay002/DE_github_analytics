PR_CONTRACT = {
    "id": {
        "type": int,
        "nullable": False,
    },
    "number": {
        "type": int,
        "nullable": False,
    },
    "state": {
        "type": str,
        "nullable": False,
        "allowed_values": {"open", "closed"},
    },
    "updated_at": {
        "type": str,
        "nullable": False,
    },
    "merged_at": {
        "type": str,
        "nullable": True,
    },
    "draft": {
        "type": bool,
        "nullable": True,
    },
}



# data = {
#     "id": None,
#     "number": 123,
#     "state": "archived",
#     "updated_at": "2026-10-03T10:00:00Z",
#     "merged_at": None,
#     "draft": False,
# }

def validate_pr_contract(data):
    errors = []
    warnings = []

    for field, rules in PR_CONTRACT.items():

        # 1. Field must exist
        if field not in data:
            errors.append(f"{field}: field is missing")
            continue

        value = data[field]

        # 2. Check nullability
        if value is None:
            if not rules["nullable"]:
                errors.append(f"{field}: cannot be null")

            # No reason to check type/domain if value is None
            continue

        # 3. Check type
        expected_type = rules["type"]

        if not isinstance(value, expected_type):
            errors.append(
                f"{field}: expected {expected_type.__name__}, "
                f"got {type(value).__name__}"
            )
            continue

        # 4. Check expected domain
        allowed_values = rules.get("allowed_values")

        if allowed_values is not None and value not in allowed_values:
            warnings.append(
                f"{field}: unexpected value '{value}'. "
                f"Expected one of {allowed_values}"
            )

    return errors, warnings


data = {
    "id": None,
    "number": 123,
    "state": "archived",
    "updated_at": "2026-10-03T10:00:00Z",
    "merged_at": None,
    "draft": False,
}


errors, warnings = validate_pr_contract(data)

print("Errors:", errors)
print("Warnings:", warnings)