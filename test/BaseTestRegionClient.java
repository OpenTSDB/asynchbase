package org.hbase.async;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.hbase.async.generated.RPCPB;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
           "ch.qos.*", "org.slf4j.*",
           "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class, Channels.class,
  RPCPB.ResponseHeader.class, NotServingRegionException.class, Config.class,
  RegionInfo.class, RPCPB.ExceptionResponse.class, HBaseRpc.class })
public class BaseTestRegionClient {
  protected static final String host = "127.0.0.1";
  protected static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  protected static final byte[] KEY = { 'k', 'e', 'y' };
  protected static final byte[] FAMILY = { 'f' };
  protected static final byte[] HRPC3 = new byte[] { 'h', 'r', 'p', 'c', 3 };
  protected static final byte SERVER_VERSION_UNKNWON = 0;

  protected final static RegionInfo region = mkregion("table", "table,,1234567890");
  
  protected HBaseRpc rpc = mock(HBaseRpc.class);
  protected Channel chan = mock(Channel.class, Mockito.RETURNS_DEEP_STUBS);
  protected ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
  protected ChannelStateEvent cse = mock(ChannelStateEvent.class);
  protected HBaseClient hbase_client;
  protected Config config = new Config();


  @Before
  public void before() throws Exception {
    hbase_client = mock(HBaseClient.class);
    when(hbase_client.getConfig()).thenReturn(config);
    
    PowerMockito.doAnswer(new Answer<RegionClient>(){
      @Override
      public RegionClient answer(InvocationOnMock invocation) throws Throwable {
        final Object[] args = invocation.getArguments();
        final String endpoint = (String)args[0] + ":" + (Integer)args[1];
        final RegionClient rc = mock(RegionClient.class);
        when(rc.getRemoteAddress()).thenReturn(endpoint);
        return rc;
      }
    }).when(hbase_client, "newClient", anyString(), anyInt());
  }
  
  // Helpers //
  
  private static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(), HBaseClient.EMPTY_ARRAY);
  }

}
