package com.globalsight.ling.tm3.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultilingualTm extends TM3Tests {

    @BeforeClass
    public static void setup() throws Exception {
        init();
    }
    
    // Set up a bilingual TM for each test, start a fresh hibernate session, etc
    @Before
    public void beforeTest() throws Exception {
        currentSession = sessionFactory.openSession();
        Transaction tx = null;
        try {
            tx = currentSession.beginTransaction();
            System.out.println("Creating TM id " + currentTestId);
            TM3Tm<TestData> tm = manager.createMultilingualTm(
                    currentSession, FACTORY, inlineAttrs());
            currentSession.flush();
            currentTestId = tm.getId();
            currentTestEvent = tm.addEvent(0, "test", "test " + currentTestId);
            tx.commit();
        }
        catch (Exception e) {
            tx.rollback();
            throw e;
        }
    }
    
    @After
    public void afterTest() throws Exception {
        super.afterTest();
    }
    
    @Test
    public void testCreateMultilingualTm() throws Exception {
        Transaction tx = null;
        try {
            tx = currentSession.beginTransaction();            
            TM3Tm<TestData> tm2 = manager.getTm(currentSession, FACTORY, currentTestId);
            assertNotNull(tm2);
            assertTrue(tm2 instanceof MultilingualTm);
            
            cleanupTestDb(manager);
            
            tx = currentSession.beginTransaction();
            TM3Tm<TestData> tm3 = manager.getTm(currentSession, FACTORY, currentTestId);
            assertNull(tm3);
            tx.commit();
        }
        catch (Exception e) {
            tx.rollback();
            throw e;
        }
    }
    
    
}
