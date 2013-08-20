package com.globalsight.ling.tm3.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultilingualSharedTm extends TM3Tests {

    static final long SHARED_STORAGE_ID = 1;
    
    @BeforeClass
    public static void setup() throws Exception {
        init();
        currentSession = sessionFactory.openSession();
        TM3Manager<?> mgr = DefaultManager.create(currentSession);
        mgr.removeStoragePool(SHARED_STORAGE_ID);
        // Recreate it
        mgr.createStoragePool(SHARED_STORAGE_ID, inlineAttrs());
        currentSession.close();
    }
    
    // Set up a bilingual TM for each test, start a fresh hibernate session, etc
    @Before
    @Override
    public void beforeTest() throws Exception {
        super.beforeTest();
        Transaction tx = null;
        try {
            tx = currentSession.beginTransaction();
            System.out.println("Creating TM id " + currentTestId);
            TM3Tm<TestData> tm = manager.createMultilingualSharedTm(
                    FACTORY, inlineAttrs(), SHARED_STORAGE_ID);
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
    public void testCreateMultilingualSharedTm() throws Exception {
        Transaction tx = null;
        try {
            TM3Tm<TestData> tm2 = manager.getTm(FACTORY, currentTestId);
            assertNotNull(tm2);
            assertTrue(tm2 instanceof MultilingualSharedTm);
            
            cleanupTestDb(manager);
            
            TM3Tm<TestData> tm3 = manager.getTm(FACTORY, currentTestId);
            assertNull(tm3);
        }
        catch (Exception e) {
            tx.rollback();
            throw e;
        }
    }
    
}
