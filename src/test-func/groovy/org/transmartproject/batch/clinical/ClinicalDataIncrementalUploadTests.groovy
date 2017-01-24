package org.transmartproject.batch.clinical

import org.junit.AfterClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.RuleChain
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.transmartproject.batch.beans.GenericFunctionalTestConfiguration
import org.transmartproject.batch.beans.PersistentContext
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.junit.JobRunningTestTrait
import org.transmartproject.batch.junit.RunJobRule
import org.transmartproject.batch.support.TableLists

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.contains
import static org.hamcrest.Matchers.containsInAnyOrder
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasEntry
import static org.hamcrest.Matchers.hasSize
import static org.hamcrest.Matchers.is
import static org.transmartproject.batch.matchers.IsInteger.isIntegerNumber
import static org.transmartproject.batch.support.StringUtils.escapeForLike

/**
 * Tests incremental clinical data upload.
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class ClinicalDataIncrementalUploadTests implements JobRunningTestTrait {

    public static final String STUDY_ID = 'GSE8581'

    @ClassRule
    public final static TestRule RUN_JOB_RULE = new RuleChain([
            new RunJobRule("${STUDY_ID}_simple_inc_upload", 'clinical', ['-n', '-d', 'INCREMENTAL=Y']),
            new RunJobRule("${STUDY_ID}_simple", 'clinical'),
    ])

    @AfterClass
    static void cleanDatabase() {
        PersistentContext.truncator.
                truncate(TableLists.CLINICAL_TABLES + 'ts_batch.batch_job_instance')
    }

    @Test
    void testNumberOfObservationFacts() {
        long numFacts = rowCounter.count(
                Tables.OBSERVATION_FACT,
                'sourcesystem_cd = :ss',
                ss: STUDY_ID)

        long observationsNumber = 12
        assertThat numFacts, is(observationsNumber)
    }

    @Test
    void testNumberOfPatients() {
        long numFacts = rowCounter.count(
                Tables.PATIENT_DIMENSION,
                'sourcesystem_cd LIKE :pat',
                pat: "$STUDY_ID:%")

        long observationsNumber = 10
        assertThat numFacts, is(observationsNumber)
    }

    @Test
    void testIncrementalUploadForExistingConcept() {
        def conceptName = 'FEV1'

        def q = """
            SELECT DISTINCT P.sourcesystem_cd, O.nval_num
            FROM ${Tables.OBSERVATION_FACT} O
            INNER JOIN ${Tables.CONCEPT_DIMENSION} C on C.concept_cd = O.concept_cd
            INNER JOIN ${Tables.PATIENT_DIMENSION} P on P.patient_num = O.patient_num
            WHERE C.name_char = :c"""

        def r = queryForList q, [c: conceptName]
        def countList = queryForList("""
                SELECT patient_count
                FROM $Tables.CONCEPT_COUNTS
                WHERE concept_path LIKE :path_expr ESCAPE '\\'""",
                [path_expr: "%${conceptName}\\\\"], Long)

        int num = 10
        //FIXME
        //assertThat countList, contains(isIntegerNumber(num))
        assertThat r, allOf(
                hasSize(num),
                //TODO Check values
        )
    }

    @Test
    void testIncrementalUploadForNewConcept() {
        def conceptName = 'NEW VAR'

        def q = """
            SELECT DISTINCT P.sourcesystem_cd, O.nval_num
            FROM ${Tables.OBSERVATION_FACT} O
            INNER JOIN ${Tables.CONCEPT_DIMENSION} C on C.concept_cd = O.concept_cd
            INNER JOIN ${Tables.PATIENT_DIMENSION} P on P.patient_num = O.patient_num
            WHERE C.name_char = :c"""

        def r = queryForList q, [c: conceptName]
        def countList = queryForList("""
                SELECT patient_count
                FROM $Tables.CONCEPT_COUNTS
                WHERE concept_path LIKE :path_expr ESCAPE '\\'""",
                [path_expr: "%${conceptName}\\\\"], Long)

        int num = 2
        assertThat countList, contains(isIntegerNumber(num))
        assertThat r, allOf(
                hasSize(num),
                //TODO Check values
        )
    }

}
