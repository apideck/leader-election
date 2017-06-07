package io.valadan.consul;

import java.util.Optional;

import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.KeyValueConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.SessionConsulClient;
import com.ecwid.consul.v1.session.model.NewSession;

public class LeaderElectionUtil {

    
    private final SessionConsulClient  sessionConsulClient = new SessionConsulClient();
    private final KeyValueConsulClient keyValueConsulClient = new KeyValueConsulClient();

    public LeaderElectionUtil() {
       
    }
    
    public Optional<String> electNewLeaderForService(final String serviceName, final String info) {
        final String key = getServiceKey(serviceName);
        String sessionId = createSession(serviceName);
        if(acquireLock(key, info, sessionId)){
            return Optional.of(info);
        } else {
            return getLeaderInfoForService(serviceName);
        }
    }

    public Optional<String> getLeaderInfoForService(final String serviceName) {
      String key = getServiceKey(serviceName);
      Response<GetValue> response = keyValueConsulClient.getKVValue(key);
      Optional<GetValue> getValue = Optional.ofNullable(response.getValue());
      return getValue.map(val -> val.getDecodedValue());
    }

    public boolean releaseLockForService(final String serviceName) {
        final String key = getServiceKey(serviceName);
        Optional<GetValue> getValue = Optional.ofNullable(keyValueConsulClient.getKVValue(key).getValue());
        Optional<Boolean> released = getValue.map(val -> val.getSession()).map(session -> releaseLock(key, session));
        
        if (released.isPresent()) {
        	return released.get();
        }
        
        return true;
    }
    
    private boolean acquireLock(String key, String info, String session) {
    	PutParams putParams = new PutParams();
    	putParams.setAcquireSession(session);
    	Response<Boolean> response = keyValueConsulClient.setKVValue(key, info, putParams);
    	return response.getValue();
    }
    
    private boolean releaseLock(String key, String session) {
    	PutParams putParams = new PutParams();
    	putParams.setReleaseSession(session);
    	Response<Boolean> response = keyValueConsulClient.setKVValue(key, "", putParams);
    	return response.getValue();    	
    }

    private String createSession(String serviceName) {
    	final NewSession newSession = new NewSession();
    	newSession.setName(serviceName);
    	Response<String> response = sessionConsulClient.sessionCreate(newSession, QueryParams.DEFAULT);
    	return response.getValue();
    }

    private String getServiceKey(String serviceName) {
        return "service/" + serviceName + "/leader";
    }

}