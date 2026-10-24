from contract import validate_pr_contract


# 1. Critical error: id cannot be NULL
test_1 = {
    "id": None,
    "number": 101,
    "state": "open",
    "updated_at": "2026-10-03T10:00:00Z",
    "merged_at": None,
    "draft": False,
}

errors, warnings = validate_pr_contract(test_1)

print("TEST 1 - NULL ID")
print("Errors:", errors)
print("Warnings:", warnings)


# 2. Warning: unexpected state
test_2 = {
    "id": 123,
    "number": 102,
    "state": "archived",
    "updated_at": "2026-10-03T10:00:00Z",
    "merged_at": None,
    "draft": False,
}

errors, warnings = validate_pr_contract(test_2)

print("\nTEST 2 - UNKNOWN STATE")
print("Errors:", errors)
print("Warnings:", warnings)


# 3. Valid: merged_at is allowed to be NULL
test_3 = {
    "id": 124,
    "number": 103,
    "state": "open",
    "updated_at": "2026-10-03T10:00:00Z",
    "merged_at": None,
    "draft": False,
}

errors, warnings = validate_pr_contract(test_3)

print("\nTEST 3 - NULL MERGED_AT")
print("Errors:", errors)
print("Warnings:", warnings)