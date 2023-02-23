import pandas as pd
import yaml


def format_package_links(package_name, repo_link):
    if "http" not in repo_link:
        repo_link = f"https://github.com/{repo_link}/"

    return f'<a href="{repo_link}">{package_name}</a>'


def generate_plugin_table():
    plugins = yaml.safe_load(open("plugins.yaml", "rb"))
    plugin_df = pd.DataFrame(plugins).fillna("")
    plugin_df["Package Name"] = plugin_df.apply(lambda x: format_package_links(x["name"], x["repo"]), axis=1)
    plugin_df = plugin_df.rename(columns={"description": "Description", "drivers": "Drivers"})

    plugin_df.to_html(
        "source/plugin-list.html",
        escape=False,
        justify="left",
        index=False,
        columns=["Package Name", "Description", "Drivers"],
    )


if __name__ == "__main__":
    print("Generating custom plugin table... ", end="")
    generate_plugin_table()
    print("done")
