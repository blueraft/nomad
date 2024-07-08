import os
import sys
import requests
import pandas as pd
from packaging import version


def get_env_var(var_name):
    value = os.getenv(var_name)
    if value is None:
        print(f'Missing {var_name} environment variable')
        sys.exit(1)
    return value


CI_JOB_TOKEN = get_env_var('CI_JOB_TOKEN')
CI_PROJECT_ID = get_env_var('CI_PROJECT_ID')

NB_PACKAGES_PER_PAGE = 100


def fetch_packages():
    packages = []
    hasNext = True
    nextLink = f'https://gitlab.mpcdf.mpg.de/api/v4/projects/{CI_PROJECT_ID}/packages?page=1&per_page={NB_PACKAGES_PER_PAGE}&order_by=created_at&sort=asc'
    job_token = {'JOB-TOKEN': CI_JOB_TOKEN}
    while hasNext and nextLink:
        response = requests.get(nextLink, headers=job_token)
        print(response)

        if response.status_code != 200:
            print('Unable to list Gitlab packages, no cleanup can be done')
            sys.exit(1)

        packages.extend(response.json())
        nextLink = None
        links = response.headers.get('Link', '')
        if 'rel="next"' in links:
            links_parts = links.split(',')
            for part in links_parts:
                if 'rel="next"' in part:
                    nextLink = part[part.find('<') + 1 : part.find('>')]
                    break
        hasNext = nextLink is not None

    return pd.DataFrame(packages)


def find_packages_to_delete(packages: pd.DataFrame):
    grouped = packages.groupby('name')

    packages_to_delete = []

    for _, group in grouped:
        # Convert version strings to version objects for comparison
        group['parsed_version'] = group['version'].apply(version.parse)
        sorted_group = group.sort_values(by='parsed_version', ascending=False)
        # Find the non-dev versions
        latest_non_dev = sorted_group[
            ~sorted_group['parsed_version'].apply(lambda x: x.is_prerelease)
        ]
        if len(latest_non_dev) < 2:
            continue
        # Find the second latest non dev version. (eg: given 1.3.3, 1.3.2, 1.3.1, we want: 1.3.2)
        second_latest_non_dev = latest_non_dev.iloc[1]
        second_latest_non_dev_version = second_latest_non_dev['parsed_version']
        # Add dev versions older than the second latest non-dev version to the delete list
        # (eg: 1.3.3, 1.3.3.dev123, 1.3.2, 1.3.2.dev456, 1.3.1, 1.3.1.dev678 -> [1.3.2.dev456, 1.3.1.dev678])
        dev_versions_to_delete = sorted_group[
            (sorted_group['parsed_version'] < second_latest_non_dev_version)
            & (sorted_group['parsed_version'].apply(lambda x: x.is_prerelease))
        ]
        packages_to_delete.extend(dev_versions_to_delete.to_dict('records'))
    return packages_to_delete


if __name__ == '__main__':
    packages = fetch_packages()
    packages_to_delete = find_packages_to_delete(packages)
    df = pd.DataFrame(packages_to_delete)
    print('packages to delete', df)
