package com.aerospike;

public class QueryTest implements QueryRunnable {

    private WorkloadProvider provider;
    /**
     * @param provider
     */
    @Override
    public void setWorkloadProvider(WorkloadProvider provider) {
        this.provider = provider;
    }

    /**
     * @return
     */
    @Override
    public String getDescription() {
        return "Test Query";
    }

    /**
     * @return
     */
    @Override
    public boolean preProcess() {
        System.out.println("PreProcess QueryTest");
        System.out.println(provider);
        return true;
    }

    /**
     *
     */
    @Override
    public void postProcess() {
        System.out.println("PostProcess QueryTest");
        System.out.println(provider);
    }

    /**
     *
     */
    @Override
    public Boolean call() throws InterruptedException {
        System.out.println("Running QueryTest");
        System.out.println(provider);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            System.out.println("QueryTest Exception " + e.getMessage());
            throw e;
        }
        return true;
    }
}
