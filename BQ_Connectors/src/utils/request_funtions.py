import requests
from src.ETL.Loading import LoaderBQ
from src.ETL.Extraction import GeneralRequest
from src.ETL.Transformation import TransformationDask


def pagination_getresponse(creds, key_ind):
    extractor = GeneralRequest(request_func=requests.request, params=creds[key_ind]['extract_keys'])
    response = extractor.run_requests()

    transformer = TransformationDask(raw_data=response, creds=creds[key_ind]['transform_keys'])
    transformed_data = transformer.run_transformation()

    loader = LoaderBQ(creds=creds[key_ind]['load_keys'], data=transformed_data)
    loader.execute_loading()

    pages = int(response.headers['totalpages'])

    if creds[key_ind]['extract_keys']['params']['page'] < pages:
        creds[key_ind]['extract_keys']['params']['page'] += 1
        return pagination_getresponse(creds=creds, key_ind=key_ind)


def pagination_zendesk(creds, key_ind):
    extractor = GeneralRequest(request_func=requests.request, params=creds[key_ind]['extract_keys'])
    response = extractor.run_requests()

    transformer = TransformationDask(raw_data=response, creds=creds[key_ind]['transform_keys'])
    transformed_data = transformer.run_transformation()

    loader = LoaderBQ(creds=creds[key_ind]['load_keys'], data=transformed_data)
    loader.execute_loading()

    next_page = response.json()['next_page']

    if next_page:
        creds[key_ind]['extract_keys']['url'] = next_page.split('&')[0]
        return pagination_zendesk(creds=creds, key_ind=key_ind)


def pagination_pushwoosh(creds, key_ind):
    extractor = GeneralRequest(request_func=requests.request, params=creds[key_ind]['extract_keys'])
    response = extractor.run_requests()

    transformer = TransformationDask(raw_data=response, creds=creds[key_ind]['transform_keys'])
    transformed_data = transformer.run_transformation()

    loader = LoaderBQ(creds=creds[key_ind]['load_keys'], data=transformed_data)
    loader.execute_loading()

    pagination_token = response.json()['pagination_token']

    if pagination_token:
        creds[key_ind]['extract_keys']['json']['pagination_token'] = pagination_token
        return pagination_pushwoosh(creds=creds, key_ind=key_ind)
