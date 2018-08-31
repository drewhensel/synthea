package org.mitre.synthea.export;

import static org.mitre.synthea.export.ExportHelper.dateFromTimestamp;
import static org.mitre.synthea.export.ExportHelper.shortDateFromTimestamp;
import static org.mitre.synthea.export.ExportHelper.iso8601Timestamp;
import static org.mitre.synthea.export.ExportHelper.randomSeconds;

import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.UUID;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.lang.String;

import org.apache.xpath.operations.Bool;
import org.mitre.synthea.helpers.Utilities;
import org.mitre.synthea.helpers.Config;
import org.mitre.synthea.world.agents.Person;
import org.mitre.synthea.world.concepts.HealthRecord;
import org.mitre.synthea.world.concepts.HealthRecord.CarePlan;
import org.mitre.synthea.world.concepts.HealthRecord.Code;
import org.mitre.synthea.world.concepts.HealthRecord.Encounter;
import org.mitre.synthea.world.concepts.HealthRecord.Entry;
import org.mitre.synthea.world.concepts.HealthRecord.ImagingStudy;
import org.mitre.synthea.world.concepts.HealthRecord.Medication;
import org.mitre.synthea.world.concepts.HealthRecord.Observation;
import org.mitre.synthea.world.concepts.HealthRecord.Procedure;

/**
 * Researchers have requested a simple table-based format
 * that could easily be imported into any database for analysis.
 * Unlike other formats which export a single record per patient,
 * this format generates 9 total files,
 * and adds lines to each based on the clinical events for each patient.
 * These files are intended to be analogous to database tables,
 * with the patient UUID being a foreign key.
 * Files include:
 * patients.csv, encounters.csv, allergies.csv,
 * medications.csv, conditions.csv, careplans.csv,
 * observations.csv, procedures.csv, and immunizations.csv.
 */
public class CSVExporter {
  /**
   * Writer for patients.csv.
   */
  private FileWriter patients;
  /**
   * Writer for allergies.csv.
   */
  private FileWriter allergies;
  /**
   * Writer for medications.csv.
   */
  private FileWriter medications;
  /**
   * Writer for conditions.csv.
   */
  private FileWriter conditions;
  /**
   * Writer for careplans.csv.
   */
  private FileWriter careplans;
  /**
   * Writer for observations.csv.
   */
  private FileWriter observations;
  /**
   * Writer for procedures.csv.
   */
  private FileWriter procedures;
  /**
   * Writer for immunizations.csv.
   */
  private FileWriter immunizations;
  /**
   * Writer for encounters.csv.
   */
  private FileWriter encounters;
  /**
   * Writer for imaging_studies.csv
   */
  private FileWriter imagingStudies;
  /**
   * Writer for vital-observation.csv.
  **/
  private FileWriter vitals;

  /**
   * Writer for social-determinant.csv.
   **/
  private FileWriter socialdet;

  
  /**
   * System-dependent string for a line break. (\n on Mac, *nix, \r\n on Windows)
   */
  private static final String NEWLINE = System.lineSeparator();

  /**
   * Determine which output formats you would like to use
   */
  private static final Boolean comcastOutput = Boolean.parseBoolean(Config.get("generate.comcast_output"));
  private static final Boolean parseAddress = Boolean.parseBoolean(Config.get("generate.parse_address"));

  /**
   * Constructor for the CSVExporter -
   *  initialize the 9 specified files and store the writers in fields.
   */
  private CSVExporter() {
    try {
      File output = Exporter.getOutputFolder("csv", null);
      output.mkdirs();
      Path outputDirectory = output.toPath();
      File patientsFile = outputDirectory.resolve("patients.csv").toFile();
      File allergiesFile = outputDirectory.resolve("allergies.csv").toFile();
      File medicationsFile = outputDirectory.resolve("medication.csv").toFile();
      File conditionsFile = outputDirectory.resolve("condition.csv").toFile();
      File careplansFile = outputDirectory.resolve("careplan.csv").toFile();
      File observationsFile = outputDirectory.resolve("lab-observation.csv").toFile();
      File proceduresFile = outputDirectory.resolve("procedure.csv").toFile();
      File immunizationsFile = outputDirectory.resolve("immunization.csv").toFile();
      File encountersFile = outputDirectory.resolve("encounter.csv").toFile();
      File vitalsFile = outputDirectory.resolve("vital-observation.csv").toFile();
      File socialDetFile = outputDirectory.resolve("social-determinant.csv").toFile();
      File imagingStudiesFile = outputDirectory.resolve("imaging_studies.csv").toFile();
  
      patients = new FileWriter(patientsFile);
      allergies = new FileWriter(allergiesFile);
      medications = new FileWriter(medicationsFile);
      conditions = new FileWriter(conditionsFile);
      careplans = new FileWriter(careplansFile);
      observations = new FileWriter(observationsFile);
      procedures = new FileWriter(proceduresFile);
      immunizations = new FileWriter(immunizationsFile);
      encounters = new FileWriter(encountersFile);
      vitals = new FileWriter(vitalsFile);
      socialdet = new FileWriter(socialDetFile);
      imagingStudies = new FileWriter(imagingStudiesFile);
      writeCSVHeaders();
    } catch (IOException e) {
      // wrap the exception in a runtime exception.
      // the singleton pattern below doesn't work if the constructor can throw
      // and if these do throw ioexceptions there's nothing we can do anyway
      throw new RuntimeException(e);
    }
  }

  /**
   * Write the headers to each of the CSV files.
   * @throws IOException if any IO error occurs
   */
  private void writeCSVHeaders() throws IOException {
    if(comcastOutput){
      patients.write("ID,BIRTHDATE,DEATHDATE,SSN,DRIVERS,PASSPORT,"
              + "PREFIX,FIRST,LAST,SUFFIX,MAIDEN,MARITAL,RACE,ETHNICITY,GENDER,BIRTHPLACE,STREETADDRESS1,STREETADDRESS2,CITY,STATE,POSTAL,COUNTRY");
      patients.write(NEWLINE);

      allergies.write("START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION");
      allergies.write(NEWLINE);

      medications.write("MRN,Timestamp,EncounterID,StartDate,EndDate,MedicationDuration,RXNormCode,RXNormDescription,MedicationReasonCode,MedicationReason");
      medications.write(NEWLINE);

      conditions.write("MRN,Timestamp,EncounterID,StartDate,EndDate,ConditionDuration,ConditionCode,ConditionDescription");
      conditions.write(NEWLINE);

      careplans.write(
              "MRN,Timestamp,CarePlanID,EncounterID,StartDate,EndDate,CarePlanDuration,CarePlanCode,CarePlanDescription,CarePlanReasonCode,CarePlanReason");
      careplans.write(NEWLINE);

      observations.write("MRN,Timestamp,LabDate,LOINCCode,LOINCDescription,LabValue,LabUnits,EncounterID");
      observations.write(NEWLINE);

      procedures.write("MRN,Timestamp,ProcedureDate,ProcedureCode,ProcedureDescription,ProcedureReasonCode,ProcedureReason,EncounterID");
      procedures.write(NEWLINE);

      immunizations.write("MRN,Timestamp,ImmunizationDate,CVXCode,CVXDescription,EncounterID");
      immunizations.write(NEWLINE);

      encounters.write("MRN,Timestamp,EncounterID,EncounterDate,EncounterCode,EncounterType,EncounterReasonCode,EncounterReason");
      encounters.write(NEWLINE);

      vitals.write("MRN,Timestamp,VitalDate,LOINCCode,LOINCDescription,VitalValue,VitalUnits,EncounterID");
      vitals.write(NEWLINE);

      socialdet.write("MRN,Timestamp,SDDate,LOINCCode,LOINCDescription,SDValue,SDUnits,EncounterID");
      socialdet.write(NEWLINE);

      imagingStudies.write("ID,DATE,PATIENT,ENCOUNTER,BODYSITE_CODE,BODYSITE_DESCRIPTION,"
              + "MODALITY_CODE,MODALITY_DESCRIPTION,SOP_CODE,SOP_DESCRIPTION");
      imagingStudies.write(NEWLINE);
    }
    else{
      if(parseAddress){
        patients.write("ID,BIRTHDATE,DEATHDATE,SSN,DRIVERS,PASSPORT,"
                + "PREFIX,FIRST,LAST,SUFFIX,MAIDEN,MARITAL,RACE,ETHNICITY,GENDER,BIRTHPLACE,"
                + "STREETADDRESS1,STREETADDRESS2,CITY,STATE,ZIP,COUNTRY");
      }
      else{
        patients.write("ID,BIRTHDATE,DEATHDATE,SSN,DRIVERS,PASSPORT,"
                + "PREFIX,FIRST,LAST,SUFFIX,MAIDEN,MARITAL,RACE,ETHNICITY,GENDER,BIRTHPLACE,"
                + "ADDRESS,CITY,STATE,ZIP");
      }
      patients.write(NEWLINE);
      allergies.write("START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION");
      allergies.write(NEWLINE);
      medications.write(
              "START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,COST,DISPENSES,TOTALCOST,"
                      + "REASONCODE,REASONDESCRIPTION"
      );
      medications.write(NEWLINE);
      conditions.write("START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION");
      conditions.write(NEWLINE);
      careplans.write(
              "ID,START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,REASONCODE,REASONDESCRIPTION");
      careplans.write(NEWLINE);
      observations.write("DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,VALUE,UNITS,TYPE");
      observations.write(NEWLINE);
      procedures.write("DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,COST,REASONCODE,REASONDESCRIPTION");
      procedures.write(NEWLINE);
      immunizations.write("DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,COST");
      immunizations.write(NEWLINE);
      encounters.write("ID,START,STOP,PATIENT,CODE,DESCRIPTION,COST,REASONCODE,REASONDESCRIPTION");
      encounters.write(NEWLINE);
      imagingStudies.write("ID,DATE,PATIENT,ENCOUNTER,BODYSITE_CODE,BODYSITE_DESCRIPTION,"
              + "MODALITY_CODE,MODALITY_DESCRIPTION,SOP_CODE,SOP_DESCRIPTION");
      imagingStudies.write(NEWLINE);
    }

  }
  
  /**
   *  Thread safe singleton pattern adopted from
   *  https://stackoverflow.com/questions/7048198/thread-safe-singletons-in-java
   */
  private static class SingletonHolder {
    /**
     * Singleton instance of the CSVExporter.
     */
    private static final CSVExporter instance = new CSVExporter();
  }

  /**
   * Get the current instance of the CSVExporter.
   * @return the current instance of the CSVExporter.
   */
  public static CSVExporter getInstance() {
    return SingletonHolder.instance;
  }

  /**
   * Add a single Person's health record info to the CSV records.
   * @param person Person to write record data for
   * @param time Time the simulation ended
   * @throws IOException if any IO error occurs
   */
  public void export(Person person, long time) throws IOException {
    String personID = patient(person, time);

    for (Encounter encounter : person.record.encounters) {
      String encounterID = encounter(personID, encounter);

      for (HealthRecord.Entry condition : encounter.conditions) {
        condition(personID, encounterID, condition);
      }

      for (HealthRecord.Entry allergy : encounter.allergies) {
        allergy(personID, encounterID, allergy);
      }

      for (Observation observation : encounter.observations) {
        observation(personID, encounterID, observation);
      }

      for (Procedure procedure : encounter.procedures) {
        procedure(personID, encounterID, procedure);
      }

      for (Medication medication : encounter.medications) {
        medication(personID, encounterID, medication, time);
      }

      for (HealthRecord.Entry immunization : encounter.immunizations) {
        immunization(personID, encounterID, immunization);
      }

      for (CarePlan careplan : encounter.careplans) {
        careplan(personID, encounterID, careplan);
      }

      for (Observation observation : encounter.observations) {
        vitals(personID, encounterID, observation);
      }

      for (Observation observation : encounter.observations) {
        socialdet(personID, encounterID, observation);
      }

      for (ImagingStudy imagingStudy : encounter.imagingStudies) {
        imagingStudy(personID, encounterID, imagingStudy);
      }
    }
    
    patients.flush();
    encounters.flush();
    conditions.flush();
    allergies.flush();
    medications.flush();
    careplans.flush();
    observations.flush();
    procedures.flush();
    immunizations.flush();
    vitals.flush();
    socialdet.flush();
    imagingStudies.flush();
  }

  /**
   * Write a single Patient line, to patients.csv.
   *
   * @param person Person to write data for
   * @param time Time the simulation ended, to calculate age/deceased status
   * @return the patient's ID, to be referenced as a "foreign key" if necessary
   * @throws IOException if any IO error occurs
   */
  private String patient(Person person, long time) throws IOException {
    // Default output
    // ID,BIRTHDATE,DEATHDATE,SSN,DRIVERS,PASSPORT,PREFIX,
    // FIRST,LAST,SUFFIX,MAIDEN,MARITAL,RACE,ETHNICITY,GENDER,BIRTHPLACE,ADDRESS
    // Comcast output
    // ID,BIRTHDATE,DEATHDATE,SSN,DRIVERS,PASSPORT,PREFIX,
    // FIRST,LAST,SUFFIX,MAIDEN,MARITAL,RACE,ETHNICITY,GENDER,BIRTHPLACE,STREETADDRESS1,
    // STREETADDRESS2,CITY,STATE,POSTAL,COUNTRY
    StringBuilder s = new StringBuilder();

    String personID = (String) person.attributes.get(Person.ID);

    if (comcastOutput){

      s.append(personID).append(',');
      s.append(dateFromTimestamp((long)person.attributes.get(Person.BIRTHDATE))).append(',');
      if (!person.alive(time)) {
        s.append(dateFromTimestamp(person.record.death));
      }

      for (String attribute : new String[] {
              Person.IDENTIFIER_SSN,
              Person.IDENTIFIER_DRIVERS,
              Person.IDENTIFIER_PASSPORT,
              Person.NAME_PREFIX,
              Person.FIRST_NAME,
              Person.LAST_NAME,
              Person.NAME_SUFFIX,
              Person.MAIDEN_NAME,
              Person.MARITAL_STATUS,
              Person.RACE,
              Person.ETHNICITY,
              Person.GENDER,
              Person.BIRTHPLACE
      }) {
        String value = (String) person.attributes.getOrDefault(attribute, "");
        s.append(',').append(clean(value));
      }

      // Call function to parse address
      String newAddress = parsedAddress(person.attributes.get(Person.ADDRESS).toString(), person.attributes.get(Person.CITY).toString(),
                          person.attributes.get(Person.STATE).toString(), person.attributes.get(Person.ZIP).toString());

      s.append(newAddress);

      s.append(NEWLINE);
      write(s.toString(), patients);

    }
    else{
      s.append(personID).append(',');
      s.append(dateFromTimestamp((long)person.attributes.get(Person.BIRTHDATE))).append(',');
      if (!person.alive(time)) {
        s.append(dateFromTimestamp(person.record.death));
      }

      for (String attribute : new String[] {
              Person.IDENTIFIER_SSN,
              Person.IDENTIFIER_DRIVERS,
              Person.IDENTIFIER_PASSPORT,
              Person.NAME_PREFIX,
              Person.FIRST_NAME,
              Person.LAST_NAME,
              Person.NAME_SUFFIX,
              Person.MAIDEN_NAME,
              Person.MARITAL_STATUS,
              Person.RACE,
              Person.ETHNICITY,
              Person.GENDER,
              Person.BIRTHPLACE
      }) {
        String value = (String) person.attributes.getOrDefault(attribute, "");
        s.append(',').append(clean(value));
      }

      // Call function to parse address if set in config file
      if(parseAddress) {
        String newAddress = parsedAddress(person.attributes.get(Person.ADDRESS).toString(), person.attributes.get(Person.CITY).toString(),
                person.attributes.get(Person.STATE).toString(), person.attributes.get(Person.ZIP).toString());

        s.append(newAddress);
      }
      else{
        s.append(',');
        s.append(person.attributes.get(Person.ADDRESS).toString()).append(",");
        s.append(person.attributes.get(Person.CITY).toString()).append(",");
        s.append(person.attributes.get(Person.STATE).toString()).append(",");
        s.append(person.attributes.get(Person.ZIP).toString());
      }

      s.append(NEWLINE);
      write(s.toString(), patients);
    }

    return personID;
  }

  /**
   * Write a single Encounter line to encounters.csv.
   *
   * @param personID The ID of the person that had this encounter
   * @param encounter The encounter itself
   * @return The encounter ID, to be referenced as a "foreign key" if necessary
   * @throws IOException if any IO error occurs
   */
  private String encounter(String personID, Encounter encounter) throws IOException {
    // Original: ID,START,STOP,PATIENT,CODE,DESCRIPTION,COST,REASONCODE,REASONDESCRIPTION
    // Comcast: MRN,Timestamp,EncounterID,EncounterDate,EncounterCode,EncounterType,EncounterReasonCode,EncounterReason
    StringBuilder s = new StringBuilder();

    String encounterID = UUID.randomUUID().toString();

    if (comcastOutput){
      s.append(personID).append(',');

      // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
      String timeStamp = randomSeconds(dateFromTimestamp(encounter.start));

      s.append(timeStamp).append(',');
      //s.append(randomSeconds(iso8601Timestamp(encounter.start))).append(',');
    /* Need to review this new handling of timestamps
    s.append(iso8601Timestamp(encounter.start)).append(',');
    if (encounter.stop != 0L) {
      s.append(iso8601Timestamp(encounter.stop)).append(',');
    } else {
      s.append(',');
    }
    */
      s.append(encounterID).append(',');
      s.append(shortDateFromTimestamp(encounter.start)).append(',');


      Code coding = encounter.codes.get(0);
      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');
      // new cost function to review
      //s.append(String.format("%.2f", encounter.cost())).append(',');

      if (encounter.reason == null) {
        s.append(','); // reason code & desc
      } else {
        s.append(encounter.reason.code).append(',');
        s.append(clean(encounter.reason.display));
      }

      s.append(NEWLINE);
      write(s.toString(), encounters);
    }
    else{
      s.append(encounterID).append(',');
      s.append(iso8601Timestamp(encounter.start)).append(',');
      if (encounter.stop != 0L) {
        s.append(iso8601Timestamp(encounter.stop)).append(',');
      } else {
        s.append(',');
      }
      s.append(personID).append(',');

      Code coding = encounter.codes.get(0);
      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');

      s.append(String.format("%.2f", encounter.cost())).append(',');

      if (encounter.reason == null) {
        s.append(','); // reason code & desc
      } else {
        s.append(encounter.reason.code).append(',');
        s.append(clean(encounter.reason.display));
      }

      s.append(NEWLINE);
      write(s.toString(), encounters);
    }

    return encounterID;
  }

  /**
   * Write a single Condition to conditions.csv.
   *
   * @param personID ID of the person that has the condition.
   * @param encounterID ID of the encounter where the condition was diagnosed
   * @param condition The condition itself
   * @throws IOException if any IO error occurs
   */
  private void condition(String personID, String encounterID,
      Entry condition) throws IOException {
    // Default: START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION
    // Comcast: MRN,Timestamp,EncounterID,StartDate,EndDate,ConditionDuration,ConditionCode,ConditionDescription
    StringBuilder s = new StringBuilder();

    s.append(personID).append(',');
    if(comcastOutput){
      // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
      String timeStamp = randomSeconds(dateFromTimestamp(condition.start));
      s.append(timeStamp).append(',');
      s.append(encounterID).append(',');
      String startdate = shortDateFromTimestamp(condition.start);
      s.append(startdate).append(',');

      if (condition.stop != 0L) {
        String enddate = shortDateFromTimestamp(condition.stop);
        s.append(enddate).append(',');

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

        //convert String to LocalDate
        LocalDate localDateStart = LocalDate.parse(startdate, formatter);
        LocalDate localDateEnd = LocalDate.parse(enddate, formatter);

        long days = ChronoUnit.DAYS.between(localDateStart, localDateEnd);
        s.append(days).append(',');
      }
      else {
        s.append(',');
        s.append("999").append(',');
      }

      Code coding = condition.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display));

      s.append(NEWLINE);
      write(s.toString(), conditions);
    }
    else{
      s.append(dateFromTimestamp(condition.start)).append(',');
      if (condition.stop != 0L) {
        s.append(dateFromTimestamp(condition.stop));
      }
      s.append(',');
      s.append(personID).append(',');
      s.append(encounterID).append(',');

      Code coding = condition.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display));

      s.append(NEWLINE);
      write(s.toString(), conditions);
    }

  }

  /**
   * Write a single Allergy to allergies.csv.
   *
   * @param personID ID of the person that has the allergy.
   * @param encounterID ID of the encounter where the allergy was diagnosed
   * @param allergy The allergy itself
   * @throws IOException if any IO error occurs
   */
  private void allergy(String personID, String encounterID,
      Entry allergy) throws IOException {
    // START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION
    StringBuilder s = new StringBuilder();

    s.append(dateFromTimestamp(allergy.start)).append(',');
    if (allergy.stop != 0L) {
      s.append(dateFromTimestamp(allergy.stop));
    }
    s.append(',');
    s.append(personID).append(',');
    s.append(encounterID).append(',');

    Code coding = allergy.codes.get(0);

    s.append(coding.code).append(',');
    s.append(clean(coding.display));

    s.append(NEWLINE);
    write(s.toString(), allergies);
  }

  /**
   * Write a single Observation to observations.csv.
   *
   * @param personID ID of the person to whom the observation applies.
   * @param encounterID ID of the encounter where the observation was taken
   * @param observation The observation itself
   * @throws IOException if any IO error occurs
   */
  private void observation(String personID, String encounterID,
      Observation observation) throws IOException {

    if (observation.value == null) {
      if (observation.observations != null && !observation.observations.isEmpty()) {
        // just loop through the child observations

        for (Observation subObs : observation.observations) {
          observation(personID, encounterID, subObs);
        }
      }

      // no value so nothing more to report here
      return;
    }
    // MRN,Timestamp,LabDate,LOINCCode,LOINCDescription,LabValue,LabUnits,EncounterID
    // Check to see if the code is NOT one of the vitals codes for height, weight, BMI, and systolic/diastolic blood pressure
    Code coding = observation.codes.get(0);
    if (!(coding.code.equals("8302-2")) && !(coding.code.equals("29463-7")) && !(coding.code.equals("39156-5"))
            && !(coding.code.equals("8462-4")) && !(coding.code.equals("8480-6")) && !(coding.code.equals("8331-1"))
            && !(coding.code.equals("69453-9")) && !(coding.code.equals("76690-7")) && !(coding.code.equals("55277-8"))
            && !(coding.code.equals("28245-9")) && !(coding.code.equals("71802-3")) && !(coding.code.equals("63513-6"))
            && !(coding.code.equals("46240-8")) && !(coding.code.equals("72106-8"))) {

      StringBuilder s = new StringBuilder();

      if(comcastOutput){
        s.append(personID).append(',');
        // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
        String timeStamp = randomSeconds(dateFromTimestamp(observation.start));
        s.append(timeStamp).append(',');
        s.append(shortDateFromTimestamp(observation.start)).append(',');
        s.append(coding.code).append(',');
        s.append(clean(coding.display)).append(',');
        String value = ExportHelper.getObservationValue(observation);
        s.append(value).append(',');
        s.append(observation.unit).append(',');
        s.append(encounterID);

        s.append(NEWLINE);
        write(s.toString(), observations);
      }
      else{
        s.append(dateFromTimestamp(observation.start)).append(',');
        s.append(personID).append(',');
        s.append(encounterID).append(',');

        //Code coding = observation.codes.get(0);

        s.append(coding.code).append(',');
        s.append(clean(coding.display)).append(',');

        String value = ExportHelper.getObservationValue(observation);
        String type = ExportHelper.getObservationType(observation);
        s.append(value).append(',');
        s.append(observation.unit).append(',');
        s.append(type);

        s.append(NEWLINE);
        write(s.toString(), observations);
      }
    }
  }

  /**
   * Write a single Vital to vital-observation.csv.
   *
   * @param personID ID of the person to whom the observation applies.
   * @param encounterID ID of the encounter where the observation was taken
   * @param observation The observation itself
   * @throws IOException if any IO error occurs
   */
  private void vitals(String personID, String encounterID,
                      Observation observation) throws IOException {

    if (observation.value == null) {
      if (observation.observations != null && !observation.observations.isEmpty()) {
        // just loop through the child observations

        for (Observation subObs : observation.observations) {
          vitals(personID, encounterID, subObs);
        }
      }

      // no value so nothing more to report here
      return;
    }

    // MRN,Timestamp,VitalDate,LOINCCode,LOINCDescription,VitalValue,VitalUnits,EncounterID

    // Check to see if the code is one of the vitals codes for height, weight, BMI, and systolic/diastolic blood pressure
    Code coding = observation.codes.get(0);

    if (coding.code.equals("8302-2") || coding.code.equals("29463-7") || coding.code.equals("39156-5") ||
            coding.code.equals("8462-4") || coding.code.equals("8480-6") || coding.code.equals("8331-1")) {

      StringBuilder s = new StringBuilder();

      if(comcastOutput){
        s.append(personID).append(',');
        // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
        String timeStamp = randomSeconds(dateFromTimestamp(observation.start));
        s.append(timeStamp).append(',');
        s.append(shortDateFromTimestamp(observation.start)).append(',');
        s.append(coding.code).append(',');
        s.append(clean(coding.display)).append(',');
        String value = ExportHelper.getObservationValue(observation);
        s.append(value).append(',');
        s.append(observation.unit).append(',');
        s.append(encounterID);

        // investigate what type is
        // String type = ExportHelper.getObservationType(observation);
        // s.append(type);

        s.append(NEWLINE);
        write(s.toString(), vitals);
      }
      else{
        s.append(dateFromTimestamp(observation.start)).append(',');
        s.append(personID).append(',');
        s.append(encounterID).append(',');

        //Code coding = observation.codes.get(0);

        s.append(coding.code).append(',');
        s.append(clean(coding.display)).append(',');

        String value = ExportHelper.getObservationValue(observation);
        String type = ExportHelper.getObservationType(observation);
        s.append(value).append(',');
        s.append(observation.unit).append(',');
        s.append(type);

        s.append(NEWLINE);
        write(s.toString(), vitals);
      }

    }
  }

  /**
   * Write a single Social Determinant to social-determinant.csv.
   *
   * @param personID ID of the person to whom the observation applies.
   * @param encounterID ID of the encounter where the observation was taken
   * @param observation The observation itself
   * @throws IOException if any IO error occurs
   */
  private void socialdet(String personID, String encounterID,
                      Observation observation) throws IOException {

    if (observation.value == null) {
      if (observation.observations != null && !observation.observations.isEmpty()) {
        // just loop through the child observations

        for (Observation subObs : observation.observations) {
          socialdet(personID, encounterID, subObs);
        }
      }

      // no value so nothing more to report here
      return;
    }

    // MRN,Timestamp,VitalDate,LOINCCode,LOINCDescription,VitalValue,VitalUnits,EncounterID

    // Check to see if the code is one of the vitals codes for height, weight, BMI, and systolic/diastolic blood pressure
    Code coding = observation.codes.get(0);

    if (coding.code.equals("69453-9") || coding.code.equals("76690-7") || coding.code.equals("55277-8")
            || coding.code.equals("28245-9") || coding.code.equals("71802-3") || coding.code.equals("63513-6")
            || coding.code.equals("46240-8") || coding.code.equals("72106-8")) {

      StringBuilder s = new StringBuilder();

      if(comcastOutput){
        s.append(personID).append(',');
        // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
        String timeStamp = randomSeconds(dateFromTimestamp(observation.start));
        s.append(timeStamp).append(',');
        s.append(shortDateFromTimestamp(observation.start)).append(',');
        s.append(coding.code).append(',');
        s.append(clean(coding.display)).append(',');
        String value = ExportHelper.getObservationValue(observation);
        s.append(value).append(',');
        s.append(observation.unit).append(',');
        s.append(encounterID);

        s.append(NEWLINE);
        write(s.toString(), socialdet);
      }
      else{
        s.append(dateFromTimestamp(observation.start)).append(',');
        s.append(personID).append(',');
        s.append(encounterID).append(',');

        //Code coding = observation.codes.get(0);

        s.append(coding.code).append(',');
        s.append(clean(coding.display)).append(',');

        String value = ExportHelper.getObservationValue(observation);
        String type = ExportHelper.getObservationType(observation);
        s.append(value).append(',');
        s.append(observation.unit).append(',');
        s.append(type);

        s.append(NEWLINE);
        write(s.toString(), socialdet);
      }
    }
  }

  /**
   * Write a single Procedure to procedures.csv.
   *
   * @param personID ID of the person on whom the procedure was performed.
   * @param encounterID ID of the encounter where the procedure was performed
   * @param procedure The procedure itself
   * @throws IOException if any IO error occurs
   */
  private void procedure(String personID, String encounterID,
      Procedure procedure) throws IOException {
    // Old: DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,REASONCODE,REASONDESCRIPTION
    // New: MRN,Timestamp,ProcedureDate,ProcedureCode,ProcedureDescription,ProcedureReasonCode,ProcedureReason,EncounterID
    StringBuilder s = new StringBuilder();

    if(comcastOutput){
      s.append(personID).append(',');
      // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
      String timeStamp = randomSeconds(dateFromTimestamp(procedure.start));
      s.append(timeStamp).append(',');
      s.append(shortDateFromTimestamp(procedure.start)).append(',');

      Code coding = procedure.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');

      if (procedure.reasons.isEmpty()) {
        s.append(','); // reason code & desc
        s.append(',');
      } else {
        Code reason = procedure.reasons.get(0);
        s.append(reason.code).append(',');
        s.append(clean(reason.display)).append(',');
      }
      s.append(encounterID);

      // investigate cost
      // s.append(String.format("%.2f", procedure.cost())).append(',');

      s.append(NEWLINE);
      write(s.toString(), procedures);
    }
    else{
      s.append(dateFromTimestamp(procedure.start)).append(',');
      s.append(personID).append(',');
      s.append(encounterID).append(',');

      Code coding = procedure.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');

      s.append(String.format("%.2f", procedure.cost())).append(',');

      if (procedure.reasons.isEmpty()) {
        s.append(','); // reason code & desc
      } else {
        Code reason = procedure.reasons.get(0);
        s.append(reason.code).append(',');
        s.append(clean(reason.display));
      }

      s.append(NEWLINE);
      write(s.toString(), procedures);
    }
  }

  /**
   * Write a single Medication to medications.csv.
   * 
   * @param personID ID of the person prescribed the medication.
   * @param encounterID ID of the encounter where the medication was prescribed
   * @param medication The medication itself
   * @param stopTime End time
   * @throws IOException if any IO error occurs
   */
  private void medication(String personID, String encounterID,
      Medication medication, long stopTime) throws IOException {
    // Original: START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,
    // COST,DISPENSES,TOTALCOST,REASONCODE,REASONDESCRIPTION
    // Comcast: MRN,Timestamp,EncounterID,StartDate,EndDate,MedicationDuration,RXNormCode,RXNormDescription,MedicationReasonCode,MedicationReason
    StringBuilder s = new StringBuilder();

    if(comcastOutput){
      s.append(personID).append(',');
      // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
      String timeStamp = randomSeconds(dateFromTimestamp(medication.start));
      s.append(timeStamp).append(',');
      s.append(encounterID).append(',');

      String startdate = shortDateFromTimestamp(medication.start);
      s.append(startdate).append(',');

      if (medication.stop != 0L) {
        String enddate = shortDateFromTimestamp(medication.stop);
        s.append(enddate).append(',');

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

        //convert String to LocalDate
        LocalDate localDateStart = LocalDate.parse(startdate, formatter);
        LocalDate localDateEnd = LocalDate.parse(enddate, formatter);

        long days = ChronoUnit.DAYS.between(localDateStart, localDateEnd);
        s.append(days).append(',');
      }
      else {
        s.append(',');
        s.append("999").append(',');
      }

      Code coding = medication.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');

      // Commenting out cost section for Comcast timeline
      /*BigDecimal cost = medication.cost();
      s.append(cost).append(',');
      long dispenses = 1; // dispenses = refills + original
      // makes the math cleaner and more explicit. dispenses * unit cost = total cost

      long stop = medication.stop;
      if (stop == 0L) {
        stop = stopTime;
      }
      long medDuration = stop - medication.start;

      if (medication.prescriptionDetails != null
              && medication.prescriptionDetails.has("refills")) {
        dispenses = medication.prescriptionDetails.get("refills").getAsInt();
      } else if (medication.prescriptionDetails != null
              && medication.prescriptionDetails.has("duration")) {
        JsonObject duration = medication.prescriptionDetails.getAsJsonObject("duration");

        long quantity = duration.get("quantity").getAsLong();
        String unit = duration.get("unit").getAsString();
        long durationMs = Utilities.convertTime(unit, quantity);
        dispenses = medDuration / durationMs;
      } else {
        // assume 1 refill / month
        long durationMs = Utilities.convertTime("months", 1);
        dispenses = medDuration / durationMs;
      }

      if (dispenses < 1) {
        // integer division could leave us with 0,
        // ex. if the active time (start->stop) is less than the provided duration
        // or less than a month if no duration provided
        dispenses = 1;
      }

      s.append(dispenses).append(',');
      BigDecimal totalCost = cost
              .multiply(BigDecimal.valueOf(dispenses))
              .setScale(2, RoundingMode.DOWN); // truncate to 2 decimal places
      s.append(totalCost).append(',');
      */
      if (medication.reasons.isEmpty()) {
        s.append(','); // reason code & desc
      } else {
        Code reason = medication.reasons.get(0);
        s.append(reason.code).append(',');
        s.append(clean(reason.display));
      }

      s.append(NEWLINE);
      write(s.toString(), medications);
    }
    else{
      s.append(dateFromTimestamp(medication.start)).append(',');
      if (medication.stop != 0L) {
        s.append(dateFromTimestamp(medication.stop));
      }
      s.append(',');
      s.append(personID).append(',');
      s.append(encounterID).append(',');

      Code coding = medication.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');

      BigDecimal cost = medication.cost();
      s.append(cost).append(',');
      long dispenses = 1; // dispenses = refills + original
      // makes the math cleaner and more explicit. dispenses * unit cost = total cost

      long stop = medication.stop;
      if (stop == 0L) {
        stop = stopTime;
      }
      long medDuration = stop - medication.start;

      if (medication.prescriptionDetails != null
              && medication.prescriptionDetails.has("refills")) {
        dispenses = medication.prescriptionDetails.get("refills").getAsInt();
      } else if (medication.prescriptionDetails != null
              && medication.prescriptionDetails.has("duration")) {
        JsonObject duration = medication.prescriptionDetails.getAsJsonObject("duration");

        long quantity = duration.get("quantity").getAsLong();
        String unit = duration.get("unit").getAsString();
        long durationMs = Utilities.convertTime(unit, quantity);
        dispenses = medDuration / durationMs;
      } else {
        // assume 1 refill / month
        long durationMs = Utilities.convertTime("months", 1);
        dispenses = medDuration / durationMs;
      }

      if (dispenses < 1) {
        // integer division could leave us with 0,
        // ex. if the active time (start->stop) is less than the provided duration
        // or less than a month if no duration provided
        dispenses = 1;
      }

      s.append(dispenses).append(',');
      BigDecimal totalCost = cost
              .multiply(BigDecimal.valueOf(dispenses))
              .setScale(2, RoundingMode.DOWN); // truncate to 2 decimal places
      s.append(totalCost).append(',');

      if (medication.reasons.isEmpty()) {
        s.append(','); // reason code & desc
      } else {
        Code reason = medication.reasons.get(0);
        s.append(reason.code).append(',');
        s.append(clean(reason.display));
      }

      s.append(NEWLINE);
      write(s.toString(), medications);
    }
  }

  /**
   * Write a single Immunization to immunizations.csv.
   *
   * @param personID ID of the person on whom the immunization was performed.
   * @param encounterID ID of the encounter where the immunization was performed
   * @param immunization The immunization itself
   * @throws IOException if any IO error occurs
   */
  private void immunization(String personID, String encounterID,
      Entry immunization) throws IOException  {
    // Old: DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION
    // New: MRN,Timestamp,ImmunizationDate,CVXCode,CVXDescription,EncounterID
    StringBuilder s = new StringBuilder();

    if(comcastOutput){
      s.append(personID).append(',');
      // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
      String timeStamp = randomSeconds(dateFromTimestamp(immunization.start));
      s.append(timeStamp).append(',');
      s.append(shortDateFromTimestamp(immunization.start)).append(',');

      Code coding = immunization.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');

      // cost added
      // s.append(String.format("%.2f", immunization.cost()));

      s.append(NEWLINE);
      write(s.toString(), immunizations);
    }
    else{
      s.append(dateFromTimestamp(immunization.start)).append(',');
      s.append(personID).append(',');
      s.append(encounterID).append(',');

      Code coding = immunization.codes.get(0);

      s.append(coding.code).append(',');
      s.append(clean(coding.display)).append(',');

      s.append(String.format("%.2f", immunization.cost()));

      s.append(NEWLINE);
      write(s.toString(), immunizations);
    }

  }

  /**
   * Write a single CarePlan to careplans.csv.
   *
   * @param personID ID of the person prescribed the careplan.
   * @param encounterID ID of the encounter where the careplan was prescribed
   * @param careplan The careplan itself
   * @throws IOException if any IO error occurs
   */
  private String careplan(String personID, String encounterID,
      CarePlan careplan) throws IOException {
    // Original: ID,START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,REASONCODE,REASONDESCRIPTION
    // Comcast: MRN,Timestamp,CarePlanID,EncounterID,StartDate,EndDate,CarePlanDuration,CarePlanCode,
    // CarePlanDescription,CarePlanReasonCode,CarePlanReason
    StringBuilder s = new StringBuilder();
    
    String careplanID = UUID.randomUUID().toString();

    if(comcastOutput){
      s.append(personID).append(',');
      // generate random seconds for each record as Comcast timeline product doesn't like identical timestamps
      String timeStamp = randomSeconds(dateFromTimestamp(careplan.start));
      s.append(timeStamp).append(',');
      s.append(careplanID).append(',');
      s.append(encounterID).append(',');

      String startdate = shortDateFromTimestamp(careplan.start);
      s.append(startdate).append(',');

      if (careplan.stop != 0L) {
        String enddate = shortDateFromTimestamp(careplan.stop);
        s.append(enddate).append(',');

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

        //convert String to LocalDate
        LocalDate localDateStart = LocalDate.parse(startdate, formatter);
        LocalDate localDateEnd = LocalDate.parse(enddate, formatter);

        long days = ChronoUnit.DAYS.between(localDateStart, localDateEnd);
        s.append(days).append(',');
      }
      else {
        s.append(',');
        s.append("999").append(',');
      }

      Code coding = careplan.codes.get(0);

      s.append(coding.code).append(',');
      s.append(coding.display).append(',');

      if (careplan.reasons.isEmpty()) {
        s.append(','); // reason code & desc
      } else {
        Code reason = careplan.reasons.get(0);
        s.append(reason.code).append(',');
        s.append(clean(reason.display));
      }
      s.append(NEWLINE);

      write(s.toString(), careplans);
    }
    else{
      s.append(careplanID).append(',');
      s.append(dateFromTimestamp(careplan.start)).append(',');
      if (careplan.stop != 0L) {
        s.append(dateFromTimestamp(careplan.stop));
      }
      s.append(',');
      s.append(personID).append(',');
      s.append(encounterID).append(',');

      Code coding = careplan.codes.get(0);

      s.append(coding.code).append(',');
      s.append(coding.display).append(',');

      if (careplan.reasons.isEmpty()) {
        s.append(','); // reason code & desc
      } else {
        Code reason = careplan.reasons.get(0);
        s.append(reason.code).append(',');
        s.append(clean(reason.display));
      }
      s.append(NEWLINE);

      write(s.toString(), careplans);
    }

    return careplanID;
  }

  /**
   * Write a single ImagingStudy to imaging_studies.csv.
   *
   * @param personID ID of the person the ImagingStudy was taken of.
   * @param encounterID ID of the encounter where the ImagingStudy was performed
   * @param imagingStudy The ImagingStudy itself
   * @throws IOException if any IO error occurs
   */
  private String imagingStudy(String personID, String encounterID,
      ImagingStudy imagingStudy) throws IOException {
    // ID,DATE,PATIENT,ENCOUNTER,BODYSITE_CODE,BODYSITE_DESCRIPTION,
    // MODALITY_CODE,MODALITY_DESCRIPTION,SOP_CODE,SOP_DESCRIPTION
    StringBuilder s = new StringBuilder();

    String studyID = UUID.randomUUID().toString();

    if(comcastOutput){
      s.append(studyID).append(',');
      s.append(dateFromTimestamp(imagingStudy.start)).append(',');
      s.append(personID).append(',');
      s.append(encounterID).append(',');

      ImagingStudy.Series series1 = imagingStudy.series.get(0);
      ImagingStudy.Instance instance1 = series1.instances.get(0);

      Code bodySite = series1.bodySite;
      Code modality = series1.modality;
      Code sopClass = instance1.sopClass;

      s.append(bodySite.code).append(',');
      s.append(bodySite.display).append(',');

      s.append(modality.code).append(',');
      s.append(modality.display).append(',');

      s.append(sopClass.code).append(',');
      s.append(sopClass.display);

      s.append(NEWLINE);

      write(s.toString(), imagingStudies);
    }
    else{
      s.append(studyID).append(',');
      s.append(dateFromTimestamp(imagingStudy.start)).append(',');
      s.append(personID).append(',');
      s.append(encounterID).append(',');

      ImagingStudy.Series series1 = imagingStudy.series.get(0);
      ImagingStudy.Instance instance1 = series1.instances.get(0);

      Code bodySite = series1.bodySite;
      Code modality = series1.modality;
      Code sopClass = instance1.sopClass;

      s.append(bodySite.code).append(',');
      s.append(bodySite.display).append(',');

      s.append(modality.code).append(',');
      s.append(modality.display).append(',');

      s.append(sopClass.code).append(',');
      s.append(sopClass.display);

      s.append(NEWLINE);

      write(s.toString(), imagingStudies);
    }

    return studyID;
  }

  /**
   * Replaces commas and line breaks in the source string with a single space.
   * Null is replaced with the empty string.
   */
  private static String clean(String src) {
    if (src == null) {
      return "";
    } else {
      return src.replaceAll("\\r\\n|\\r|\\n|,", " ").trim();
    }
  }

  private static String parsedAddress(String addressParts, String city, String state, String zip){

    // break street address into address 1 and 2 which are the only scenarios as of now 7/25/2018
    // NOTE: making a huge assumption this is US for country
    String[] parts = {};
    String newAddress = "";
    if (!addressParts.equals(null) && !addressParts.isEmpty())
      parts = addressParts.split("\\s+");
    else
      parts = null;

    // current scenarios
    // 3 parts: number street1 street1
    // 4 parts: number street1 street1 street1
    // 5 parts: number street1 street1 street2 street2
    // 6 parts: number street1 street1 street1 street2 street2
    if (parts.length == 3)
      newAddress = "," + clean(parts[0]) + " " + clean(parts[1]) + " " + clean(parts[2]) + ",";
    else if (parts.length == 4)
      newAddress = "," + clean(parts[0]) + " " + clean(parts[1]) + " " + clean(parts[2]) + " " + clean(parts[3]) + ",";
    else if (parts.length == 5)
      newAddress = "," + clean(parts[0]) + " " + clean(parts[1]) + " " + clean(parts[2]) + "," + clean(parts[3]) + " " + clean(parts[4]);
    else if (parts.length == 6)
      newAddress = "," + clean(parts[0]) + " " + clean(parts[1]) + " " + clean(parts[2]) + " " + clean(parts[3]) + "," + clean(parts[4]) + " " + clean(parts[5]);
    else
      newAddress = ", address contained" + String.valueOf(parts.length) + " elements,";

    // build address string for csv
    String address = newAddress
            + "," + city
            + "," + state
            + "," + zip
            + ",US";
    //s.append(clean(address));

    return address;
  }

  /**
   * Helper method to write a line to a File.
   * Extracted to a separate method here to make it a little easier to replace implementations.
   *
   * @param line The line to write
   * @param writer The place to write it
   * @throws IOException if an I/O error occurs
   */
  private static void write(String line, FileWriter writer) throws IOException {
    synchronized (writer) {
      writer.write(line);
    }
  }
}
