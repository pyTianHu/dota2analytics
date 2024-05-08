# About DotA2 by the authors
 Every day, millions of players worldwide enter the battle as one of over a hundred Dota Heroes in a 5v5 team clash. 
 Dota is the deepest multi-player action RTS game ever made and there's always a new strategy or tactic to discover. 
 It's completely free to play and always will be. <br>
 https://www.dota2.com/home

# DotA2 and Data Analytics
Since DotA2 has a very advanced in-game data collection mechanism, and this data is also served to the community, I wanted to create a data project to practice data software engineering. <br>
The data is collected via opendota API: https://docs.opendota.com/

# Supporting links
- JIRA board: https://pytian.atlassian.net/jira/software/projects/DOT/boards/2/backlog

# Goal of the project
- Ingest data from the API
- Transform the data and prepare it for analysis
- Analyze data from multiple perspective
- But mainly to practice data engineering skills so I can understand engineers better, and so become a better scrum master / BA / Team Lead

# Tools used for the project
- python 3.11
- sqlite3
- github
- jira
- vscode

# Structure
- classes: tableoperations.py contains generic operations that are to be executed against the database tables of the solution.
- logs: contains log files that collect runtime information about the jobs that are executed. Logs are generated by a custom class and its methods.
- scripts: contains the actual ingestion, transformation and load procedures that work with the data.
- tests: separate directory for files used in testing the different functions and methods.
- utils: container of utility files

# Versioning
- version notation is major.mid.minor
- major version copmrises of multiple features and requirements that are identified as part of the plan for the specified major version. There is no strict cadence for major releases, meaning, whenever all the work is done for the verison plan, the version gets released.
- mid version comprises of one or more elements that are deployed to the production branch in-between major releases.
- minor version comprises of issue and bug fixes. Once a bug or issue is identified in production that prevents the data product from running properly, it must be fixed with highest priority, and the fix then be released to production branch as soon as possible. Such cases would increment the minor version by 1.

# SDLC
## Kanban method.
- Plan is created in advance for a version by identifying the key features.
- Then, Epics are created as a container to break down features into smaller parts.
- Epics are broken down further into user stories that are presented on a kanban board, and are taken into work based on dependencies, relationships, urgency and impact and benefit.
- User stories are not estimated for effort or complexity. Priority field is used to order items - this reflects the current state of items at the given point in time (can happen that an item now is low priority, then 2 weeks later it will be critically important)

# Logic and flow
- The entry point of the application is main.py
- The project does not have an orchestration tool installed yet, so triggering the job execution is manual at this stage. 
main.py check what data it needs to pull, then starts ingestion, table creation (on-demand) and inserts data into the raw layer.
- then main.py carries out transformation from the raw layer to the bronze layer. 
- As in each release there will be new tables added into the data model, the function checks for what tables to extract data from. At the raw-to-bronze transformation level basic field selection and data insertion logics happen so this is handled in a generic mechanism.
- All methods execution results are stored in log files in the logs folder.