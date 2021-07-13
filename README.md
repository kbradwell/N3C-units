# N3C-units
Measurement unit harmonization and inference pipeline scripts, developed for the National COVID Cohort Collaborative (N3C)

## Unit inference threshold validation 
Start with the OMOP measurement table and filter down to just rows with value_as_number present, then perform an inner join on the codesets found in canonical_units_of_measure (supplementary material). You can then use the functions in the following script to receated the unit inference threshold validation results: unit_inference_threshold_validation.py

## Unit inference and unit harmonization
The pipeline implemented on the patient data can be found in the scripts beginning with PIPELINE

## Software versions
Versions of software used for validation and pipeline steps are found in the file package_versions_UH&I_ms.txt
