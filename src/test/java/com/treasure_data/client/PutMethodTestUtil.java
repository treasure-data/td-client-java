package com.treasure_data.client;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;

import org.junit.Ignore;

import com.treasure_data.model.Request;

@Ignore
public class PutMethodTestUtil extends AnyMethodTestUtil {

    @Override
    public void doBusinessLogic() throws Exception {
        throw new UnsupportedOperationException();
    }

    public void callMockDoMethodRequest() throws Exception {
        doNothing().when(conn).doPutRequest(any(Request.class),
                any(String.class), any(byte[].class));
    }
}
