import asyncio

import aiohttp
import pandas as pd
import yaml


def format_package_links(package_name, repo_link):
    return f'<a href="{repo_link}">{package_name}</a>'


def format_repo_link(repo_link):
    if "http" not in repo_link:
        return f"https://github.com/{repo_link}/"
    return repo_link


def format_badge_html(badge, link):
    return f'<a href="{link}"><img src="{badge}"/></a>'


async def check_ok(client, url):
    async with client.get(url) as r:
        if "anaconda.org" in url:
            body = await r.text()
            if "requires authentication" in body:
                return False
        return r.ok


async def check_all_ok(urls):
    async with aiohttp.client.ClientSession() as c:
        coroutines = [check_ok(c, url) for url in urls]
        return await asyncio.gather(*coroutines)


def generate_plugin_table():
    plugins = yaml.safe_load(open("plugins.yaml", "rb"))
    plugin_df = pd.DataFrame(plugins)
    plugin_df = plugin_df.rename(columns={"description": "Description", "drivers": "Drivers"})

    plugin_df["short_name"] = plugin_df["name"].apply(lambda x: x.split("/")[-1])
    plugin_df["repo_links"] = plugin_df["repo"].apply(format_repo_link)
    plugin_df["conda_package"] = plugin_df["conda_package"].fillna(plugin_df["short_name"])
    plugin_df["ci_yaml"] = plugin_df["ci_yaml"].fillna("main.yaml")

    # CI badges
    plugin_df["ci_badges"] = plugin_df[["repo_links", "ci_yaml"]].apply(
        lambda x: f"{x[0]}/actions/workflows/{x[1]}/badge.svg", axis=1
    )
    plugin_df["ci_links"] = plugin_df["repo_links"].apply(lambda x: f"{x}/actions")
    ci_badges_ok = asyncio.run(check_all_ok(plugin_df["ci_badges"]))

    # Docs badges
    plugin_df["docs_badges"] = plugin_df["short_name"].apply(
        lambda x: f"https://readthedocs.org/projects/{x}/badge/?version=latest"
    )
    plugin_df["docs_links"] = plugin_df["short_name"].apply(
        lambda x: f"https://{x}.readthedocs.io/en/latest/?badge=latest"
    )
    docs_badges_ok = asyncio.run(check_all_ok(plugin_df["docs_links"]))

    # PyPi badges
    plugin_df["pypi_badges"] = plugin_df["short_name"].apply(
        lambda x: f"https://img.shields.io/pypi/v/{x}.svg?maxAge=3600"
    )
    plugin_df["pypi_links"] = plugin_df["short_name"].apply(
        lambda x: f"https://pypi.org/project/{x}"
    )
    pypi_badges_ok = asyncio.run(check_all_ok(plugin_df["pypi_links"]))

    # Conda badges
    plugin_df["conda_badges"] = plugin_df[["conda_channel", "conda_package"]].apply(
        lambda x: f"https://img.shields.io/conda/vn/{x[0]}/{x[1]}.svg?colorB=4488ff&label={x[0]}&style=flat",
        axis=1,
    )
    plugin_df["conda_links"] = plugin_df[["conda_channel", "conda_package"]].apply(
        lambda x: f"https://anaconda.org/{x[0]}/{x[1]}", axis=1
    )
    conda_badges_ok = asyncio.run(check_all_ok(plugin_df["conda_links"]))

    # Conda defaults badges
    plugin_df["conda_defaults_links"] = plugin_df["conda_package"].apply(
        lambda x: f"https://anaconda.org/anaconda/{x}"
    )
    plugin_df["conda_defaults_badges"] = plugin_df["conda_package"].apply(
        lambda x: f"https://img.shields.io/conda/vn/anaconda/{x}.svg?colorB=4488ff&label=defaults&style=flat"
    )
    conda_defaults_badges_ok = asyncio.run(check_all_ok(plugin_df["conda_defaults_links"]))

    # Conda forge badges
    plugin_df["conda_forge_links"] = plugin_df["conda_package"].apply(
        lambda x: f"https://anaconda.org/conda-forge/{x}"
    )
    plugin_df["conda_forge_badges"] = plugin_df["conda_package"].apply(
        lambda x: f"https://img.shields.io/conda/vn/conda-forge/{x}.svg?colorB=4488ff&style=flat"
    )
    conda_forge_badges_ok = asyncio.run(check_all_ok(plugin_df["conda_forge_links"]))

    plugin_df["Package Name"] = plugin_df[["name", "repo_links"]].apply(
        lambda x: format_package_links(*x), axis=1
    )
    plugin_df["CI"] = plugin_df[["ci_badges", "ci_links"]][ci_badges_ok].apply(
        lambda x: format_badge_html(*x), axis=1
    )
    plugin_df["Docs"] = plugin_df[["docs_badges", "docs_links"]][docs_badges_ok].apply(
        lambda x: format_badge_html(*x), axis=1
    )
    plugin_df["PyPi"] = plugin_df[["pypi_badges", "pypi_links"]][pypi_badges_ok].apply(
        lambda x: format_badge_html(*x), axis=1
    )
    plugin_df["conda"] = plugin_df[["conda_badges", "conda_links"]][conda_badges_ok].apply(
        lambda x: format_badge_html(*x), axis=1
    )
    plugin_df["conda_forge"] = plugin_df[["conda_forge_badges", "conda_forge_links"]][
        conda_forge_badges_ok
    ].apply(lambda x: format_badge_html(*x), axis=1)
    plugin_df["conda_defaults"] = plugin_df[["conda_defaults_badges", "conda_defaults_links"]][
        conda_defaults_badges_ok
    ].apply(lambda x: format_badge_html(*x), axis=1)

    plugin_df = plugin_df.fillna("")

    # Concat conda badges
    plugin_df["Conda"] = plugin_df["conda"] + plugin_df["conda_forge"] + plugin_df["conda_defaults"]

    plugin_df.to_html(
        "source/plugin-list.html",
        escape=False,
        justify="left",
        index=False,
        border=0,
        classes="table_wrapper",
        columns=["Package Name", "Description", "Drivers", "CI", "Docs", "PyPi", "Conda"],
        col_space=["auto", "auto", "auto", "90px", "90px", "90px", "90px"],
    )


if __name__ == "__main__":
    print("Generating custom plugin table... ", end="")
    generate_plugin_table()
    print("done")
