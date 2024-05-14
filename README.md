# metabase-sync
This tool is designed to synchronize multiple Metabase instances.

## Background
I designed an application for my clients, each with their own instance, database, and Metabase. Since the database structure is the same, configuring Metabase was redundant. The idea was to configure on a local instance first and then send/update it to all other instances.

## Preparing the Ground
Before any copying, it's essential to prepare the ground. Not wanting to prevent my clients from creating/modifying their own Dashboards, I decided to focus on one collection.

The idea was to add a lock 🔒 to the name of the root collection that I want to control across all instances. This also helps protect it from any modifications afterward.

## HOW-TO
Here’s how to use it:
- Prepare on a first instance a collection containing a lock in the name 🔒
- In the collection, add: sub-collections, templates, questions, dashboards. Note, everything must be in this collection!
- Clone the repository
- Prepare your settings.json file following the template
- Make sure the database structures on the different instances are identical!
- Add on ```main.py``` your lines under ```ADD HERE YOUR LINES AS REQUIRED```
- Install dependencies ```pip install -r requirements.txt```
- Run the script: ```python3 main.py```

## FAQ
### How Does It Work?
The process is as follows:
- The script connects to the first instance and retrieves a list of databases that match all fields.
- It fetches a list of collections and only keeps collections with a lock 🔒 and their "child" collections.
- It retrieves questions/templates/dashboards from the collections
- It exports the structures in JSON (This helped me with debugging, so I left it in.)

Then, for synchronization:
- For each source collection, it checks if the collection is already in the target and sends a request, either to create or update as necessary.
- The same goes for questions/templates/dashboards.
- At this stage, there will likely be errors. For example, a question may call another question that has not yet been migrated. So, I could have made it more elegant, but for now, it retries if there are only errors of the type "the question depends on another question not found."

And that's it!

### What are the pitfalls?
- The script takes data from the source and sends it to the target. If data has been modified on the target, those modifications will be overwritten. So: Always modify only the source.
- The link between the objects on one side and the other is only made using the position in the collections hierarchy and the name. Therefore, if, for example, a question is renamed on the source between two synchronizations, the target will contain both questions. The old one will not be deleted.

### What are 'patterns' in the settings file?
In one of the questions, I used =concat('__pattern__', 'another thing') because I wanted the value of __pattern__ to be different for each client. So, that’s what it's for!

## TODO
⏹️ Clean up the code, as it's more of a big Hack than a neat solution right now.

⏹️ Check if there are any elements on the targets that are no longer needed and remove them.

⏹️ Add a real translating to this README.md, ChatGPT did this one ! 🤡

⏹️ Any ideas?