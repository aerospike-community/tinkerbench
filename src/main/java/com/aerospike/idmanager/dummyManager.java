package com.aerospike.idmanager;

import com.aerospike.AGSGraphTraversal;
import com.aerospike.IdManagerQuery;
import com.aerospike.LogSource;
import com.aerospike.OpenTelemetry;

public class dummyManager implements IdManagerQuery {

    public final boolean listManagers;

    public dummyManager(boolean listManagers) {
        this.listManagers = listManagers;
    }
    public dummyManager() {
        this.listManagers = false;
    }

    @Override
    public boolean enabled() { return false; }

    @Override
    public void init(AGSGraphTraversal ags,
                     OpenTelemetry openTelemetry,
                     LogSource logger,
                     String gremlinString) {
    }

    @Override
    public boolean CheckIdsExists(LogSource logger) {
        return false;
    }

    @Override
    public boolean isInitialized() {
        return false;
    }

    @Override
    public int getIdCount() {
        return 0;
    }


    @Override
    public int getStartingIdsCount() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public Object getId() {
        return null;
    }

    @Override
    public Object getId(int depth) {
        return null;
    }

    @Override
    public void Reset() {
    }

    @Override
    public int getDepth() {
        return 0;
    }

    @Override
    public int getInitialDepth() {
        return 0;
    }

    /**
     * @return
     */
    @Override
    public int getNbrRelationships() {
        return 0;
    }


    @Override
    public void setDepth(int depth) {
    }


    @Override
    public Object[] getIds() {
        return new Object[0];
    }

    @Override
    public Object[] getNewIds() { return new Object[0]; }

    @Override
    public long importFile(String filePath, OpenTelemetry openTelemetry, LogSource logger, int sampleSize, String[] labels) {
        return 0;
    }

    @Override
    public void exportFile(String filePath, LogSource logger) {
    }

    @Override
    public void printStats(final LogSource ignore) {}
}
