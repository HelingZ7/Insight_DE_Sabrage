# Sabrage
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

### [My Demo Slides](https://docs.google.com/presentation/d/1Q8tELHN3Fg_cCddaM5QqEJ05cOOhHs2ef9wk1n0puhU/edit?usp=sharing)
