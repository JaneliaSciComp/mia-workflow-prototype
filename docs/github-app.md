# Running autodev as a GitHub App (bot identity)

By default autodev uses a personal token, so everything it does on GitHub shows up as you. With a GitHub App it
acts as its own bot account, `<app-slug>[bot]`:

| what | personal token | GitHub App |
|---|---|---|
| issue/PR comments, labels | you | `<app-slug>[bot]` |
| opening the PR, pushing `autodev/issue-N` | you | `<app-slug>[bot]` |
| agent commits (author) | you | `<app-slug>[bot]`, with the bot's avatar |
| approving autodev's PRs | `/approve <sha>` comment (you can't approve your own PR) | a normal *Approve* review |

You need to be an owner of the JaneliaSciComp org, or an org owner has to make you a GitHub App manager, to create
the App and install it. Everything else is done as the account that runs autodev. Back to the
[README](../README.md). The service-account checklist is in [`notes/autodev-runbook.md`](../notes/autodev-runbook.md).

## 1. Create the App

Go to **github.com/organizations/JaneliaSciComp/settings/apps** and click **New GitHub App**.

- **GitHub App name**: this sets the bot's login, e.g. `mia-autodev` becomes `mia-autodev[bot]`. It must be unique
  across GitHub.
- **Homepage URL**: `https://github.com/JaneliaSciComp/mia-workflow-prototype`.
- **Callback URL, Setup URL, "Request user authorization"**: leave empty or unchecked. autodev never acts on
  behalf of a user.
- **Webhook**: uncheck **Active**. autodev polls, and the worker accepts no inbound connections.
- **Repository permissions**:

  | permission | access |
  |---|---|
  | Contents | Read and write |
  | Pull requests | Read and write |
  | Issues | Read and write |
  | Checks | Read-only |
  | Commit statuses | Read-only |
  | Metadata | Read-only (required) |

  Leave everything else, including **Workflows** and **Administration**, at *No access*. Without Workflows the
  bot can't change `.github/workflows/`, and autodev refuses agent changes under `.github/` anyway.
- **Organization and account permissions**: none.
- **Subscribe to events**: none.
- **Where can this GitHub App be installed?**: **Only on this account**.

Click **Create GitHub App**.

## 2. Note the App ID and generate a key

On the App's **General** page:

- Copy the **App ID**. This is a number. Don't use the *Client ID*, which starts with `Iv`.
- Under **Private keys**, click **Generate a private key**. A `.pem` file downloads.

Move the key to the account that runs autodev, and make it readable only by that account:

```bash
mkdir -p ~/.config/autodev
mv ~/Downloads/<app-name>.*.private-key.pem ~/.config/autodev/app.pem
chmod 600 ~/.config/autodev/app.pem
```

Keep the key out of the repo and out of `paths.work_root`. The config refuses `agent.extra_mounts` that overlap it,
and agent jobs never see it.

## 3. Install it on the repo

On the App's page, click **Install App**, choose **JaneliaSciComp**, then **Only select repositories**, and pick
`mia-workflow-prototype` (or whichever repos autodev should watch). Click **Install**.

You land on a page like `github.com/organizations/JaneliaSciComp/settings/installations/12345678`. The number at
the end is the **installation ID**.

## 4. Configure autodev

In `autodev.yaml`:

```yaml
github:
  repo: JaneliaSciComp/mia-workflow-prototype
  auth: app
  app_id: 123456                              # step 2
  installation_id: 12345678                   # step 3
  private_key_path: ~/.config/autodev/app.pem # step 2
```

`token_env` and `token_file` are ignored when `auth: app`. `scripts/autodev-up.sh` binds the key read-only into
the worker's sandbox. The worker signs a short-lived JWT with the key and exchanges it for an installation token,
which it renews about once an hour.

## 5. Check it

```bash
pixi run autodev check
```

The `github identity` row should show `<app-slug>[bot]` and the `github repo` row should be `ok`. If the labels
don't exist yet for this repo, run `pixi run autodev init-labels`.

## 6. Switch over

Changing the identity changes who autodev believes it is. It recognizes its own round summaries and "handled
feedback" markers by login, so on a PR that's already open under the old identity it would redo feedback it
already handled, and repost its comments. Switch when nothing is in flight:

1. Finish or close every issue labelled `<deployment>:working` or `<deployment>:review`, e.g.
   `gh issue list -R JaneliaSciComp/mia-workflow-prototype -l autodev:review`.
2. `pixi run ad restart`.

At startup the worker looks up the bot user's ID and sets the commit author on every agent workspace to
`<app-slug>[bot] <ID+<app-slug>[bot]@users.noreply.github.com>`, which is how GitHub links commits to the bot.

## 7. Make approval the real gate

Now that the bot is a separate account, protect `main`. Under **Settings → Rules → Rulesets** (or
**Branches**) on the repo:

- Require a pull request before merging, with **1 approving review**.
- **Dismiss stale approvals** when new commits are pushed.
- Don't add the App to the bypass list.

autodev only merges after an approval from someone with write access, and GitHub enforces the same rule. Reviewers
now click **Approve**. `/approve <sha>` comments still work, but they don't satisfy branch protection.

Add `.github/CODEOWNERS` covering `autodev/`, `scripts/` and `.github/` so changes to the gates need a named human
(see the runbook).

## Maintenance and troubleshooting

- **Rotate the key**: generate a new one on the App's page, replace `app.pem`, `pixi run ad restart`, then delete
  the old key on GitHub.
- **Adding permissions later**: after you change the App's permissions, an org owner has to accept the new
  permissions on the installation page before they take effect.
- **`401 ... JSON web token could not be decoded`** or **`Integration not found`**: the `app_id` is wrong, or the
  key belongs to another App.
- **`404` on `/app/installations/.../access_tokens`**: the `installation_id` is wrong, or the App was uninstalled.
- **`403 Resource not accessible by integration`**: a permission from step 1 is missing, or the repo isn't in the
  installation's repository list.
- **Commits show a plain name with no avatar**: the worker started before the switch. Restart it.

zarr-doctor still uses a personal token (`$GITHUB_TOKEN`). It doesn't read these settings.
