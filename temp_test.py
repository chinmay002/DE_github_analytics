
def filter_pr_page(data, boundary):  # #data by max_per_page page 1: [pr1,pr2,pr3,pr4,pr5]--->[pr4 ,PR5 OLD]
    valid_prs = []
    boundary_crossed = False

    for pr in data:

        updated_at = datetime.fromisoformat(
            pr.get("updated_at").replace("Z", "+00:00")
        )

        if updated_at >= boundary:
            valid_prs.append(pr)
        else:
            boundary_crossed = True
            break

    return valid_prs, boundary_crossed


def get_paginated_data(
    params = None,
    per_page=100,
    max_pages=None,
    page_filter = None,
):
    all_data = []
    page = 1

    # Don't modify the original dictionary passed by the caller
    params = params.copy() if params else {}

    while True:
        params["per_page"] = per_page
        params["page"] = page

        # data = make_api_requests(
        #     url,
        #     headers=headers,
        #     params=params
        # )  

        boundary = "2026-09-12T18:00:00Z"

        data = [
            {"id": 1, "updated_at": "2026-09-13T10:00:00Z"},
            {"id": 2, "updated_at": "2026-09-12T19:00:00Z"},
            {"id": 3, "updated_at": "2026-09-12T17:00:00Z"},
            {"id": 4, "updated_at": "2026-09-11T10:00:00Z"},
]

        if page_filter:

            valid_prs, boundary_crossed = page_filter(data,boundary)
            print(valid_prs)

        else:
            valid_prs = data
            boundary_crossed = False

        all_data.extend(valid_prs)    #[PR1,Pr2,Pr3,pr3.....]

        # GitHub returned less than a full page -> last page
        if len(data) < per_page:
            break

        # Optional development limit
        if max_pages is not None and page >= max_pages:
            break
        
        if  boundary_crossed:
            break

        

        page += 1

    return all_data


print(get_paginated_data(per_page=2,max_pages=2,page_filter=filter_pr_page))