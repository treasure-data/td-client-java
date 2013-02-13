package com.treasure_data.client;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;

import org.junit.Ignore;

import com.treasure_data.model.Request;
import com.treasure_data.model.Result;

@Ignore
public class PutMethodTestUtil<REQ extends Request<?>, RET extends Result<?>>
        extends AnyMethodTestUtil<REQ, RET> {

    public void callMockDoMethodRequest() throws Exception {
        doNothing().when(conn).doPutRequest(any(Request.class),
                any(String.class), any(byte[].class));
    }
}
