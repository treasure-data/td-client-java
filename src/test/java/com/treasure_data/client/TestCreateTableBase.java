package com.treasure_data.client;

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;

import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;

@Ignore
public class TestCreateTableBase extends
        PostMethodTestUtil<CreateTableRequest, CreateTableResult, DefaultClientAdaptorImpl> {

    protected String databaseName;
    protected String tableName;
    protected CreateTableRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Override
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        request = createRequest();
    }

    protected CreateTableRequest createRequest() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        tableName = null;
        request = null;
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        CreateTableResult result = doBusinessLogic();
        assertEquals(databaseName, result.getDatabase().getName());
        assertEquals(tableName, result.getTableName());
        assertTableType(result);
    }

    protected void assertTableType(CreateTableResult result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CreateTableResult doBusinessLogic() throws Exception {
        return clientAdaptor.createTable(request);
    }
}
