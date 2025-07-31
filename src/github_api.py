import json
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ])


def create_github_issue(diff_output, project_name, project_labels, project_assignees, repo_name, repo_user, github_token):
    if not diff_output["has_diff"]:
        logging.info("Pas de différence, pas de ticket")
        return
    url = "https://api.github.com/repos/{}/{}/issues".format(repo_user,repo_name)
    headers = {"Authorization" : "token {}".format(github_token)}
    issue_data = {
        "title": "[{}] Nouveautés dans les données open data".format(project_name),
        "body": diff_output.get("content", "No Content"),
        "labels": project_labels,
        "assignees": project_assignees,
        "type": "Data"
    }
    response = requests.post(url, headers=headers, data=json.dumps(issue_data))
    if response.status_code == 201:
        logging.info("Ticket créé avec succès.")
        logging.info("Lien vers le ticket : {}".format(response.json().get("html_url", "URL non disponible")))

    else:
        logging.error("Erreur lors de la création du ticket : {}".format(response.status_code))
        logging.error(response.text)
