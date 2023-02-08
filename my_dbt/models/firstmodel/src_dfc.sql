    SELECT 
      raw:id,
      explode(
        arrays_zip(
          from_json(raw:patients[*].patientid, 'ARRAY<STRING>'),
          from_json(raw:patients[*].petid, 'ARRAY<STRING>'), 
          from_json(raw:patients[*].carelocation.ouid, 'ARRAY<STRING>'),
          from_json(raw:patients[*]['name'], 'ARRAY<STRING>'),
          from_json(raw:patients[*]['Name'], 'ARRAY<STRING>'),
          from_json(raw:patients[*].animaltype.animalclassname, 'ARRAY<STRING>'),
          from_json(raw:patients[*].animaltype.animalclass, 'ARRAY<STRING>'),
          from_json(raw:patients[*].animaltype.animalbreed, 'ARRAY<STRING>'),
          from_json(raw:patients[*].animaltype.animalbreedname, 'ARRAY<STRING>')
        )
      ) 
    FROM semstrquery