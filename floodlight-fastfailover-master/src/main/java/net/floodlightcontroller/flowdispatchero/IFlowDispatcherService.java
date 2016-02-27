package net.floodlightcontroller.flowdispatchero;


import java.util.Map;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.routing.*;

public interface IFlowDispatcherService extends IFloodlightService {
	Map<String, String> pushRoutes(Route r1, Route r2, boolean isQos, boolean isIPv4);
	
	
}
