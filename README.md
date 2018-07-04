# Sabrage
### Building a data analysis pipeline to evaluate the performance of NLP models


**Insight Data Engineering Consulting Project**

**Company: [Amplion](http://www.amplion.com/about)**

**Company Descritpion:** Amplion makes information products for the pharmaceutical and diagnostics industry using Deep Learning approaches.

## **Description of the project:**
Amplion applies a natural language processing pipeline to clinical trial documents that contain very useful information about the presence or state of a disease. They have built Deep Learning models that allow identification of candidate biomarkers in the text and make determinations about their usefulness; at present, this information is stored in JSON format at the document level, not the candidate level, which leads to challenges with making improvements to the models, troubleshooting failures, and performing quality assurance tasks. Those challenges combine to limit the speed with which they can iterate. The existing pipeline also lacks version control of documments, models and target-synonym database limiting the ability to evaluate the performance of models in developement.

## **Deliverables:**
1. Migrate json data in MongoDB and metadata in MySQL to a single Postgres database, including extending schema.
2. Extend Python codebase to allow data to be saved to new Postgres database.
3. Build modules to supported versioning of documents, targets and models for easy inspection of performance and model development.

## **Pipeline Evolution:**
### **Existing Pipeline**
![Existing Pipeline](https://github.com/HelingZ7/Insight_DE_Sabrage/blob/master/docs/Existing_pipeline.jpg?raw=true)

### **New Pipeline - all information in one place for easier management**
![New Pipeline](https://github.com/HelingZ7/Insight_DE_Sabrage/blob/master/docs/New_pipeline_only.jpg?raw=true)

### **Tracking Document, Model and target-synonyms in the new pipeline**
![New Pipeline with Schema](https://github.com/HelingZ7/Insight_DE_Sabrage/blob/master/docs/New_pipeline.jpg?raw=true)

## Descrption of Selected Scripts
#### The python scripts uploaded are a part of an existing priviate repo of Amplion. 
### Modified_pipeline_scripts:
| Existing Script in Original Pipeline | My Contribution |
| --- | --- |
ct_pipeline  |  Implementation of model verion control (read git hash and use git hash to determine the model version ) 
ct_preprocessing  | 1. Implementation of document version control (write table document, document_version and document_xpath, and check if an incoming document already exists in the database and has been processed before). 2. Export the match results in the new postgres table with versions of document, target and model 
bmb_match  | Wrote a new class BmBRegexMatcherSabrage to override prior methods that read target and synonyms from MySQL 

### Scripts_for_debugging_initializing:
Script for debugging and initializing | My Contribution
| --- | --- |
write_bmb_tables_pg | Read data from target and target-synonym tables in MySQL and write normalize data into three tables in postgres (target, synonym and bridge table target-synonym)
write2pg_static_tables_dummy | Initialize 'static' tables (less likely to change over time) with dummy values. Five static tables: source_type, document_type, sources, model_type, models  
write2pg_static_tables | Populate 'static' tables (less likely to change over time). Five static tables: source_type, document_type, sources, model_type, models  
reset_mongo_pg | This code is used to delete all intermediate results in MongoDB and Postgres when debugging preprocess steps. Otherwise the scripts in preprocess will skip the files because they are already in postgres, MongoDB will raise error for duplicated key.
delete_all_record_pg | connect to postgres and delete all records in the static tables: source_type, document_type, sources, model_type, models. This is a function to reset the pg tables in test runs
 


### [My Demo Slides](https://docs.google.com/presentation/d/1Q8tELHN3Fg_cCddaM5QqEJ05cOOhHs2ef9wk1n0puhU/edit?usp=sharing)
