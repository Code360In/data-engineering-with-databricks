# Integrate Git control for Databricks Repos

This demo will cover the creation and management of Databricks Repos. By the end of this lesson you will be able to:
* Integrate your Workspace with a supported Git service 
* Create and manage Repos
* Add Notebooks to a Repo
* Perform various revision control operations on the Repo

Before proceeding, ensure that:
* Repos are enabled in your Workspace (**Settings&rarr;Admin Console&rarr;Workspace Settings&rarr;Advanced&rarr;Repos**)
* If you are using an exising account with a supported git provider, set up the integration now (**Settings&rarr;User Settings&rarr;Git Integration**) before sharing screen with learners to avoid leakage of credentials.
* If you are not using an existing git account, create a throw-away account with GitHub.

## Integrating git 

First mention that the Repos feature can be turned on or off by the administrator (can show this if desired). The allow list can also be configured there for improved security.

Now introduce learners to the page where they configure git integration (**Settings&rarr;User Settings&rarr;Git Integration**). Click on the **Change settings** button.

Click on the **Git provider** dropdown to display supported providers (note that **GitHub Enterprise** is currently in private preview; if someone is interested in that integration, then they would need to work with their Databricks representative to get that set up).

Select a few different ones and draw attention to the fact that the UI provides a link to instructions on crating a token based on the selection. But we will walk through this.

Select the one that applies to you.

If you have already configured git integration because you are using an existing account, proceed as follows:
* Show them (in the git provider UI) how to obtain the username
* How to create a personal access token with appropriate permissions, which is displayed in the Workspace UI based on the selection. NOTE: do NOT actually create a token!
* How they would populate the **Username** and **Token** fields
* Do NOT save any changes

If you are using a throw-away GitHub account:
* Show them where to find the account username in the GitHub UI
* How to create a personal access token with **repo** permissions. See https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
* Copy the token and transfer that value to the **Token** field in the Workspace UI
* Transfer the username to the **Username** field in the Workspace UI
* Click **Save**

## Create Repo

Remind learners of the 1:1 mapping between a Databricks Repo and a git repository. So first we need a git repository.

### Create git repository

In your git service UI:
* Create a new repository initialized with some content (for example, the **Add a README file** in GitHub); this way the repository is automatically initialized
* Capture the HTTPS URL.

### Create Databricks Repo

In the **Repos** page, click **Add Repo**. Mention that it's possible to start a Repo and add the remote URL later, but that's not the workflow we show here. Click **Clone remote Git repo** and paste the URL. THe **Git provider** and **Repo name** will be inferred though you can change the name if desired. Click **Create**.

## Add folder

With a new Repo created, navigate to it so that we can add some content. Remind learners that the UI metaphors are identical to the Workspace and that Repos is merely an extension of that to accomodate integration with git revision control.

Inside the Repo, create a folder (click on the chevron and select **Create&rarr;Folder**). Name the folder anything you like.

## Add Notebooks

Now let’s add Notebooks. Let's walk through a few different ways.

### Create new Notebook

If beginning a brand new project, you might be starting from a blank slate so in this case, creating a new Notebook might be the way to go.

On the folder you created, click the chevron and select **Create&rarr;Notebook**. From there, the workflow is identical to creating a new Notebook in the Workspace, which we have seen before.

From there, create a simple markdown cell:

    %md
    # Sample notebook
    Sample description

### Clone an existing Notebook

If you have existing Notebooks in your Workspace or another Repo, cloning is an option.

Navigate to a Notebook you have created in your Workspace. Click the chevron and select **Clone**.

You will likely want to overwrite the auto-generated value for **New Name**. For the destination, select **Repos** then navigate to the desired Repo and folder within, and click **Clone**.

### Import a Notebook

If you have existing Notebooks from others that you want to add, importing them is an option. Tell learners that they would first need to have them exported (ideally a DBC Archive).

From there, demonstrate the import workflow. On the folder you created in the Repo, click the chevron and select **Importk**. Again the workflow is similar to importing into the Workspace. If you have a DBC from that lesson then use that. Otherwise you can import by URL and use the following:

    https://docs.databricks.com/_static/notebooks/delta/quickstart-python.html

## Commit and push

We have set up a new Repo, and locally we created a new folder containing some Notebooks. Let’s push this upstream to our remote repository.

We do this from the Repo dialog. Open that from the Repos page or from one of the Notebooks inside the Repo.

Talk about the dialog and how it itemizes the changes. This is handy because sometimes by the time we finally get around to committing, we forget what we’ve changed. Selecting each change in the list provides a preview of the changes to the associated file.

We can commit each change individually or as many as we'd like. It’s good practice to logically group commits, and sometimes we forget to commit before we have made additional unrelated changes, so this option helps with that.

For each commit, we must provide a summary; be descriptive and concise. If desired, additional verbosity can be provided in the description.

Let’s commit the changes by clicking Commit & Push.

Point out that what we did here covers functionality, but doesn't necessarily reflect best practice. Things can get a little more complicated when multiple developers are collaborating on the same branch. If someone else has pushed their own changes to the remote repository since you last pulled from it, you will not be able to push your changes. You will have to pull the remote changes first, and potentially deal with any merging conflicts that can sometimes happen if conflicting changes are detected.

So, if developers must collaborate on the same branch, then pulling is important before you begin your development work.

## Best practices

Best practices advocate creating feature branches for your development. This way your changes are isolated and you are largely immune to the potential conflict situations mentioned above. This is particularly appropriate when working on features that aren’t tied tightly to development work being carried out by others in your team.

Once you complete your changes and they have been unit tested, reviewed and approved, then merging with other branches can be handled in a more controlled manner elsewhere in your process.

## Branching

To support best practices, let's create a feature branch called **dev**. Open the Repo dialog, which can be accessed from the **Repos** page or within any Notebook inside the Repo.

Click the **+** button to the right of the branch dropdown. The dropdown then becomes editable. Specify the name of the new branch. When creating a new branch, it will automatically become the active branch, though you can use the dropdown at any time to switch to a different branch.

With the development branch created and activated, we can now begin making changes as needed.

Import a new Notebook into the folder you created in the Repo. As an example you can use the following URL:

    https://docs.databricks.com/_static/notebooks/structured-streaming-python.html
    
Revisit the Repo dialog and notice how the changes are itemized before. Fill out a summary and commit and push the changes, just like we did before. The key difference is that now the main branch is unaffected by our changes. Everything we did is isolated in the **dev** branch. Verify this by switching branches back to main, using the dropdown in the Repo dialog. Notice how the new Notebook is closed, and the folder no longer contains that new Notebook.

Switch back to **dev** to see it reappear.
