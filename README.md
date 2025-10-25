# Databricks Asset Bundles with automated-workflow
Automated data workflow using Databricks Asset Bundles — includes an end-to-end DLT pipeline and interactive dashboard with CI/CD integration for seamless deployment, testing, and environment management across dev and production.

## Project Description:
This project demonstrates an end-to-end deployment of a data analytics solution for New York City taxi data using Databricks Asset Bundles and Delta Live Tables (DLT) as part of a medallion architecture.

The workflow begins by ingesting raw data for both yellow and green taxis, followed by transformation and enrichment steps in bronze, silver, and gold layers. The pipeline joins the taxi trip data with contextual datasets such as TaxiZones, PaymentTypes, and RateCodes, providing deeper insights and data quality assurance through constraints and expectations.

A comprehensive analytics dashboard is created on top of the gold layer, visualizing key metrics for New York taxi activity and trends. The project also includes a Databricks Job that orchestrates the execution of the DLT pipeline and automatically updates the linked dashboard.

The entire process—data pipelines, job orchestration, dashboard configuration, and infrastructure definitions—is managed using Databricks Asset Bundles. This approach allows for streamlined development and production deployments; after verifying success in a development environment, the same bundle can be promoted and deployed in production, ensuring consistency and traceability throughout.



### Bundle deployed at Development mode:
<img width="1624" height="107" alt="image" src="https://github.com/user-attachments/assets/2f5a52b7-6ff1-4790-bea3-4f34d4811fb3" />

### Bundle deployed at  Production mode:
<img width="1615" height="96" alt="image" src="https://github.com/user-attachments/assets/4c54052d-74db-49ce-88c0-dc3bf0aeb940" />

### Note
The production file is also uploaded check out!!
Dashboard is also uploaded in repository, Please check out!! :)

