package com.treasure_data.client;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;

import java.util.Map;

import org.junit.Ignore;

import com.treasure_data.model.Request;
import com.treasure_data.model.Result;

@Ignore
public class PutMethodTestUtil<REQ extends Request<?>, RET extends Result<?>, CLIENT extends AbstractClientAdaptor>
        extends AnyMethodTestUtil<REQ, RET, CLIENT> {

    public void callMockDoMethodRequest() throws Exception {
        doNothing().when(conn).doPutRequest(any(Request.class),
                any(String.class), any(Map.class), any(byte[].class));
    }
}
