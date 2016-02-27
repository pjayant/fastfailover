package net.floodlightcontroller.flowdispatchero;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Set;

import org.projectfloodlight.openflow.protocol.OFBarrierReply;
import org.projectfloodlight.openflow.protocol.OFBarrierRequest;
import org.projectfloodlight.openflow.protocol.OFBucket;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFFlowDelete;
import org.projectfloodlight.openflow.protocol.OFGroupAdd;
import org.projectfloodlight.openflow.protocol.OFGroupDelete;
import org.projectfloodlight.openflow.protocol.OFGroupType;
import org.projectfloodlight.openflow.protocol.OFPortConfig;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFPortMod;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.MatchField;

import org.projectfloodlight.openflow.protocol.ver10.OFPortConfigSerializerVer10;
import org.projectfloodlight.openflow.protocol.ver11.OFPortConfigSerializerVer11;
import org.projectfloodlight.openflow.protocol.ver12.OFPortConfigSerializerVer12;
import org.projectfloodlight.openflow.protocol.ver13.OFPortConfigSerializerVer13;
import org.projectfloodlight.openflow.protocol.ver14.OFPortConfigSerializerVer14;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.OFGroup;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import net.floodlightcontroller.routing.*;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.util.FlowModUtils;
import net.floodlightcontroller.topology.*;


public class FlowDispatcher implements IFloodlightModule, IOFSwitchListener, IFlowDispatcherService {
	/*
	 * The pre-defined services that we use.
	 */
	private static IOFSwitchService switchService;
	private static IRestApiService restApiService;
	private static ILinkDiscoveryService linkDiscoveryService;

	
	/*
	 * To more easily identify our flows, we will use a cookie.
	 */
	private static final U64 cookie = U64.ofRaw(0x11223344);

		/*
	 * Maintain an active Map of all the switches we care about and whether or not they
	 * are connected (i.e. ready-to-go). If they aren't connected, then we either just
	 * booted and need to wait, or there's a problem.
	 */
	private static Map<DatapathId, Boolean> switchConnected;
	private static boolean allSwitchesConnected;

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		/*
		 * Our module implements the IFastFailoverDemoService.
		 */
		Collection<Class<? extends IFloodlightService>> services = new ArrayList<Class<? extends IFloodlightService>>();
		services.add(IFlowDispatcherService.class);
		return services;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		/*
		 * We are the object that implements the IFastFailoverDemoService. Give our reference
		 * to the module loader so that any other modules can know where we are.
		 * 
		 * This will be used by the IRestApiService in its internal Map of Floodlight
		 * services. In this way, we will be able to call our service's functions
		 * (exposed through the interface) when a REST query is received by the IRestApiService.
		 */
		Map<Class<? extends IFloodlightService>, IFloodlightService> services = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		services.put(IFlowDispatcherService.class, this);
		return services;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		/* 
		 * We require the use of the IOFSwitchService to listen for switch events.
		 * We also have a REST API, so we need to have the IRestApiService loaded 
		 * before us as well. Lastly, we look at the discovered links in order to
		 * learn the ports for use in our flows. Thus, we depend on information
		 * from the ILinkDiscoveryService.
		 */
		Collection<Class<? extends IFloodlightService>> deps = new ArrayList<Class<? extends IFloodlightService>>();
		deps.add(IOFSwitchService.class);
		deps.add(IRestApiService.class);
		deps.add(ILinkDiscoveryService.class);
		return deps;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {

		System.out.print("FlowDispatcher module has init!!");
		/*
		 * Since we list the IOFSwitchService, IRestApiService, and ILinkDiscoveryService as 
		 * dependencies in getModuleDependencies(), they will be loaded before us in the module
		 * loading chain. So, it's safe to ask the context map for a reference to them.
		 */
		switchService = context.getServiceImpl(IOFSwitchService.class);
		restApiService = context.getServiceImpl(IRestApiService.class);
		linkDiscoveryService = context.getServiceImpl(ILinkDiscoveryService.class);

		/*
		 * Note, at this point, it still is not safe to call any functions defined
		 * by these services. We must wait until our startUp() function is called.
		 * The Floodlight module loader will have called their startUp() functions,
		 * by the time ours is called (since we listed them as dependencies). Thus,
		 * they will be ready-to-rock in our startUp().
		 */
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		/*
		 * We are a module that wants to register for switch events. So, we tell the
		 * IOFSwitchService that we want to be added to the callback chain. Note that
		 * we implement IOFSwitchListener, so we have the functions defined that the
		 * IOFSwitchService will call when a switch event occurs (e.g. switchAdded).
		 */
		switchService.addOFSwitchListener(this);


	}

	@Override
	public void switchAdded(DatapathId switchId) {

	}

	//@Override
	public void switchRemoved(DatapathId switchId) {
		/*
		 * Set the switch as disconnected and remove all
		 * associated links. The port might change if/when
		 * the switch comes back up.
		*/
	}

	@Override
	public void switchActivated(DatapathId switchId) {
		/*
		 * "Activated" is for transitions from slave to master.
		 * We don't require that in this module, and we assume
		 * we are always master to all switches (the default).
		 */
	}

	@Override
	public void switchPortChanged(DatapathId switchId, OFPortDesc port,
			PortChangeType type) {
		/*
		 * We won't react to switch port change events. We will be the
		 * cause of switch port changes by bringing them up and down
		 * administratively.
		 */
	}

	@Override
	public void switchChanged(DatapathId switchId) {
		/*
		 * Ditto (minus the port part).
		 */
	}

	
	//*************************************
	//
    //MOdification to the Fast Failover Demo of Floodlight
    //Implementation of pushRoutes
	//Author-SwagatBora & Jie Wang
    //Date-10/10/2015
    //
    //*************************************
	
	
	@Override
	public Map<String, String>pushRoutes(Route r1, Route r2, boolean isQos, boolean isIPv4){
		Map<String, String> message = new HashMap<String, String>();
		boolean isInserted  = false;
		Route main_route = r1;
		Route backup_route = r2;
		List<NodePortTuple> path_main = main_route.getPath();
		
		
	if(!isIPv4){
			// Non IPv4 packet, probably an ARP.
			System.out.println("------------Log----------- Inserting one time flow for non IPv4 packet.");
			if(main_route != null)
				insertArpFlow(path_main);
			} 
	else{
	
	//Route backup_route = r2;
		
	DatapathId src_main = main_route.getId().getSrc();
	//System.out.println("----------LOG-----------main_route src"+ src_main.toString());
	DatapathId dst_main = main_route.getId().getDst();
	//System.out.println("----------LOG-----------main_route dst"+ dst_main.toString());
	
	//List<NodePortTuple> path_main = main_route.getPath();
	//System.out.println("----------LOG-----------main_path"+ path_main.toString());
	
	if (isQos == true && backup_route != null) {
		DatapathId src_backup = backup_route.getId().getSrc();
		//System.out.println("----------LOG-----------backup_route src"+ src_backup.toString());
		DatapathId dst_backup = backup_route.getId().getDst();
		//System.out.println("----------LOG-----------backup_route dst"+ dst_backup.toString());
		List<NodePortTuple> path_backup = backup_route.getPath();
		//System.out.println("----------LOG-----------backup_path"+ path_backup.toString());
		
		ArrayList<NodePortTuple> start_bucket= new ArrayList<NodePortTuple>();
		start_bucket.add(path_main.get(0));
		start_bucket.add(path_main.get(1));
		start_bucket.add(path_backup.get(1));
		
		//System.out.println("----------LOG-----------start_bucket"+ start_bucket.toString());		
		
		ArrayList<NodePortTuple> end_bucket = new ArrayList<NodePortTuple>();
		end_bucket.add(path_main.get(path_main.size()-1));
		end_bucket.add(path_main.get(path_main.size()-2));
		end_bucket.add(path_backup.get(path_backup.size()-2));
		
		//System.out.println("----------LOG-----------end_bucket"+ end_bucket.toString());
		
	
			//Check main route and backup route have same source and destination switch
		if(src_main == src_backup && dst_main==dst_backup){
		 				
					insertFlows(path_main, true);
					insertFlows(path_backup, true);
				
					isInserted=insertGroups(start_bucket);
		 				//System.out.println("Start_bucket has been inserted ");
		 				//sendBarrier();
		 			if(isInserted)
		 				isInserted=insertGroups(end_bucket);
		 			//System.out.println("end_bucket has been inserted ");
		 			//insertFlows(path_main, true);
		 			//insertFlows(path_backup, true);
		 			if(isInserted)
		 			message.put("FlowInsert","True");
				}
			else
				System.out.println("source and destination doesn't match");
		

	} else {
		// Not QoS or No Backup Route.
		System.out.println("------------Log----------- No bakcup route for isQoS="+ isQos);
		if(main_route != null)
			insertFlows(path_main,false);
		}
	
		}

		return message;
	}
	

	
private boolean insertGroups(ArrayList<NodePortTuple> S){
	System.out.println("Entering InsertGroups");
			boolean pushed =false;
			DatapathId swId = S.get(0).getNodeId();
			IOFSwitch curr_sw = switchService.getSwitch(swId);
			OFFlowDelete flowDelete = curr_sw.getOFFactory().buildFlowDelete()
					.setCookie(cookie)
					.setCookieMask(U64.NO_MASK)
					.build();
			curr_sw.write(flowDelete);

			OFGroupDelete groupDelete = curr_sw.getOFFactory().buildGroupDelete()
					.setGroup(OFGroup.ANY)
					.setGroupType(OFGroupType.FF)
					.build();
			curr_sw.write(groupDelete);

			//sendBarrier(curr_sw);

			/* Add the group: fast-failover watching ports leading to dpid2a and dpid2b */
			ArrayList<OFBucket> buckets = new ArrayList<OFBucket>(2);
			buckets.add(curr_sw.getOFFactory().buildBucket()
					.setWatchPort(S.get(1).getPortId())
					.setWatchGroup(OFGroup.ZERO)
					.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildOutput()
							.setMaxLen(0xffFFffFF)
							.setPort(S.get(1).getPortId())
							.build()))
							.build());
							
			buckets.add(curr_sw.getOFFactory().buildBucket()
					.setWatchPort(S.get(2).getPortId())
					.setWatchGroup(OFGroup.ZERO)
					.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildOutput()
							.setMaxLen(0xffFFffFF)
							.setPort(S.get(2).getPortId())
							.build()))
							.build());
							
			OFGroupAdd groupAdd = curr_sw.getOFFactory().buildGroupAdd()
					.setGroup(OFGroup.of(1))
					.setGroupType(OFGroupType.FF)
					.setBuckets(buckets)
					.build();
			curr_sw.write(groupAdd);
			
		
			
			/*OFFlowAdd flowAdd = curr_sw.getOFFactory().buildFlowAdd()
					.setCookie(cookie)
					.setHardTimeout(0)
					.setIdleTimeout(0)
					.setPriority(FlowModUtils.PRIORITY_MAX)
					.setMatch(curr_sw.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.ARP)
							.setExact(MatchField.IN_PORT, S.get(0).getPortId())
							.build())
							.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildGroup()
									.setGroup(OFGroup.of(1))
									.build()))
									.build();

			curr_sw.write(flowAdd);*/

			OFFlowAdd flowAdd = curr_sw.getOFFactory().buildFlowAdd()
					.setCookie(cookie)
					.setHardTimeout(0)
					.setIdleTimeout(0)
					.setPriority(FlowModUtils.PRIORITY_MAX)
					.setMatch(curr_sw.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.IPv4)
							.setExact(MatchField.IN_PORT,  S.get(0).getPortId())
							.build())
							.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildGroup()
							.setGroup(OFGroup.of(1))
							.build()))
							.build();
			curr_sw.write(flowAdd);

		
			/*flowAdd = flowAdd.createBuilder()
					.setMatch(curr_sw.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.ARP)
							.setExact(MatchField.IN_PORT, S.get(1).getPortId())
							.build())
							.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildOutput()
									.setMaxLen(0xffFFffFF)
									.setPort( S.get(0).getPortId())
									.build()))
									.build();
			curr_sw.write(flowAdd);*/

			flowAdd = flowAdd.createBuilder()
					.setMatch(curr_sw.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.IPv4)
							.setExact(MatchField.IN_PORT, S.get(1).getPortId())
							.build())
							.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildOutput()
							.setMaxLen(0xffFFffFF)
							.setPort( S.get(0).getPortId())
							.build()))
							.build();
			curr_sw.write(flowAdd);

			
			/*flowAdd = flowAdd.createBuilder()
					.setMatch(curr_sw.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.ARP)
							.setExact(MatchField.IN_PORT, S.get(2).getPortId())
							.build())
							.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildOutput()
							.setMaxLen(0xffFFffFF)
							.setPort( S.get(0).getPortId())
							.build()))
							.build();
			curr_sw.write(flowAdd);*/

			flowAdd = flowAdd.createBuilder()
					.setMatch(curr_sw.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.IPv4)
							.setExact(MatchField.IN_PORT, S.get(2).getPortId())
							.build())
							.setActions(Collections.singletonList((OFAction) curr_sw.getOFFactory().actions().buildOutput()
							.setMaxLen(0xffFFffFF)
							.setPort( S.get(0).getPortId())
							.build()))
							.build();
			curr_sw.write(flowAdd);

			pushed = true;
			
			System.out.println("exit InsertGroups");
			return pushed;
}
	

	private boolean insertArpFlow(List<NodePortTuple> path){
		int index;
		boolean pushed = true;
		HashMap<DatapathId,ArrayList<OFPort>> LinksById= new HashMap<DatapathId,ArrayList<OFPort>>();
		for(index = 0; index< path.size();index+=2){
			DatapathId curr_switch=path.get(index).getNodeId();
			//System.out.println("DatapathId of current_switch:"+curr_switch.toString());
			OFPort port1 = path.get(index).getPortId();
			//System.out.println("Value of 1st port in link:"+port1.toString());
			OFPort port2 = path.get(index+1).getPortId();
			//System.out.println("Value of 2nd port in link:"+port2.toString());
			ArrayList<OFPort> list_port= new ArrayList<OFPort>(2);
			list_port.add(port1);
			list_port.add(port2);
			//System.out.println("Link is"+list_port.toString());
			LinksById.put(curr_switch,list_port);
		}
	
		for(DatapathId switch_temp : LinksById.keySet()){
			IOFSwitch curr_switch = switchService.getSwitch(switch_temp);
			//System.out.println(curr_switch.toString());
			OFFlowDelete flowDelete = curr_switch.getOFFactory().buildFlowDelete()
				.setCookie(cookie)
				.setCookieMask(U64.NO_MASK)
				.setMatch(curr_switch.getOFFactory().buildMatch().setExact(MatchField.ETH_TYPE, EthType.ARP).build())
				.build();
			curr_switch.write(flowDelete);
		
			OFFlowAdd flowAdd = curr_switch.getOFFactory().buildFlowAdd()
				.setCookie(cookie)
				.setHardTimeout(65535)
				.setIdleTimeout(5)
				.setPriority(FlowModUtils.PRIORITY_MAX)
				.setMatch(curr_switch.getOFFactory().buildMatch()
						.setExact(MatchField.ETH_TYPE, EthType.ARP)
						.setExact(MatchField.IN_PORT, LinksById.get(curr_switch.getId()).get(0))
						.build())
						.setActions(Collections.singletonList((OFAction) curr_switch.getOFFactory().actions().buildOutput()
								.setMaxLen(0xffFFffFF)
								.setPort(LinksById.get(curr_switch.getId()).get(1))
								.build()))
								.build();
			curr_switch.write(flowAdd);
			System.out.println("[LOG]-----------Writing FlowAdd for ARP :"+curr_switch.toString());
		
			flowAdd = curr_switch.getOFFactory().buildFlowAdd()
					.setCookie(cookie)
					.setHardTimeout(65535)
					.setIdleTimeout(1)
					.setPriority(FlowModUtils.PRIORITY_MAX)
					.setMatch(curr_switch.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.ARP)
							.setExact(MatchField.IN_PORT, LinksById.get(curr_switch.getId()).get(1))
							.build())
							.setActions(Collections.singletonList((OFAction) curr_switch.getOFFactory().actions().buildOutput()
									.setMaxLen(0xffFFffFF)
									.setPort(LinksById.get(curr_switch.getId()).get(0))
									.build()))
									.build();
				curr_switch.write(flowAdd);
			System.out.println("[LOG]-----------Writing FlowAdd for ARP :"+curr_switch.toString());
		}
		
	return pushed;
	}

	
	private boolean insertFlows(List<NodePortTuple> path, boolean qos_value){
		int index;
		boolean pushed =true;
		HashMap<DatapathId,ArrayList<OFPort>> LinksById= new HashMap<DatapathId,ArrayList<OFPort>>();
		for(index = 0; index< path.size();index+=2){
				DatapathId curr_switch=path.get(index).getNodeId();
				//System.out.println("DatapathId of current_switch:"+curr_switch.toString());
				OFPort port1 = path.get(index).getPortId();
				//System.out.println("Value of 1st port in link:"+port1.toString());
				OFPort port2 = path.get(index+1).getPortId();
				//System.out.println("Value of 2nd port in link:"+port2.toString());
				ArrayList<OFPort> list_port= new ArrayList<OFPort>(2);
				list_port.add(port1);
				list_port.add(port2);
				//System.out.println("Link is"+list_port.toString());
				LinksById.put(curr_switch,list_port);
		}
		
		for(DatapathId switch_temp : LinksById.keySet())
		{
		if(switch_temp == path.get(0).getNodeId() || switch_temp == path.get(path.size()-1).getNodeId()){
			if(!qos_value){
					IOFSwitch curr_switch = switchService.getSwitch(switch_temp);
					OFFlowDelete flowDelete = curr_switch.getOFFactory().buildFlowDelete()
					.setCookie(cookie)
					.setCookieMask(U64.NO_MASK)
					.setMatch(curr_switch.getOFFactory().buildMatch().setExact(MatchField.ETH_TYPE, EthType.IPv4).build())
					.build();
			curr_switch.write(flowDelete);

			//sendBarrier(curr_switch);

			OFFlowAdd flowAdd = curr_switch.getOFFactory().buildFlowAdd()
					.setCookie(cookie)
					.setHardTimeout(5)
					.setIdleTimeout(5)
					.setPriority(FlowModUtils.PRIORITY_MAX)
					.setMatch(curr_switch.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.IPv4)
							.setExact(MatchField.IN_PORT, LinksById.get(curr_switch.getId()).get(0))
							.build())
							.setActions(Collections.singletonList((OFAction) curr_switch.getOFFactory().actions().buildOutput()
									.setMaxLen(0xffFFffFF)
									.setPort(LinksById.get(curr_switch.getId()).get(1))
									.build()))
							.build();
			System.out.println("[LOG]-----------Writing FlowAdd for:"+curr_switch.toString());
			curr_switch.write(flowAdd);
			
			 flowAdd = curr_switch.getOFFactory().buildFlowAdd()
					.setCookie(cookie)
					.setHardTimeout(5)
					.setIdleTimeout(5)
					.setPriority(FlowModUtils.PRIORITY_MAX)
					.setMatch(curr_switch.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.IPv4)
							.setExact(MatchField.IN_PORT, LinksById.get(curr_switch.getId()).get(1))
							.build())
							.setActions(Collections.singletonList((OFAction) curr_switch.getOFFactory().actions().buildOutput()
									.setMaxLen(0xffFFffFF)
									.setPort(LinksById.get(curr_switch.getId()).get(0))
									.build()))
							.build();
			System.out.println("[LOG]-----------Writing FlowAdd for:"+curr_switch.toString());
			curr_switch.write(flowAdd);

					
						}
				}
			else if(switch_temp != path.get(0).getNodeId() && switch_temp != path.get(path.size()-1).getNodeId()){
							IOFSwitch curr_switch = switchService.getSwitch(switch_temp);
							System.out.println("[LOG]-----------Current switch value is:"+curr_switch.toString());
								OFFlowDelete flowDelete = curr_switch.getOFFactory().buildFlowDelete()
					.setCookie(cookie)
					.setCookieMask(U64.NO_MASK)
					.setMatch(curr_switch.getOFFactory().buildMatch().setExact(MatchField.ETH_TYPE, EthType.IPv4).build())
					.build();
			curr_switch.write(flowDelete);

		
			/* ARP and IPv4 from sw2a to sw3 */
			OFFlowAdd flowAdd = curr_switch.getOFFactory().buildFlowAdd()
					.setCookie(cookie)
					.setHardTimeout(0)
					.setIdleTimeout(5)
					.setPriority(FlowModUtils.PRIORITY_MAX)
					.setMatch(curr_switch.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.IPv4)
							.setExact(MatchField.IN_PORT, LinksById.get(curr_switch.getId()).get(0))
							.build())
							.setActions(Collections.singletonList((OFAction) curr_switch.getOFFactory().actions().buildOutput()
									.setMaxLen(0xffFFffFF)
									.setPort(LinksById.get(curr_switch.getId()).get(1))
									.build()))
									.build();
			System.out.println("[LOG]-----------Writing FlowAdd for:"+curr_switch.toString());
			curr_switch.write(flowAdd);

		    flowAdd = curr_switch.getOFFactory().buildFlowAdd()
					.setCookie(cookie)
					.setHardTimeout(0)
					.setIdleTimeout(5)
					.setPriority(FlowModUtils.PRIORITY_MAX)
					.setMatch(curr_switch.getOFFactory().buildMatch()
							.setExact(MatchField.ETH_TYPE, EthType.IPv4)
							.setExact(MatchField.IN_PORT, LinksById.get(curr_switch.getId()).get(1))
							.build())
							.setActions(Collections.singletonList((OFAction) curr_switch.getOFFactory().actions().buildOutput()
									.setMaxLen(0xffFFffFF)
									.setPort(LinksById.get(curr_switch.getId()).get(0))
									.build()))
									.build();
		    System.out.println("[LOG]-----------Writing FlowAdd for:"+curr_switch.toString());
			curr_switch.write(flowAdd);
				}
		
		
		}
	
	return pushed;
	}
	
	
	private void sendBarrier(IOFSwitch sw) {
		OFBarrierRequest barrierRequest = sw.getOFFactory().buildBarrierRequest()
				.build();
		ListenableFuture<OFBarrierReply> future = sw.writeRequest(barrierRequest);
		try {
			future.get(1, TimeUnit.SECONDS); /* If successful, we can discard the reply. */
		} catch (InterruptedException | ExecutionException
				| TimeoutException e) {
			//log.error("Switch {} doesn't support barrier messages? OVS should.", sw.toString());
		}
	}



	private long portDown(IOFSwitch sw) {
		long config = 0;
		switch (sw.getOFFactory().getVersion()) {
		case OF_10:
			config = OFPortConfigSerializerVer10.toWireValue(Collections.singleton(OFPortConfig.PORT_DOWN));
			break;
		case OF_11:
			config = OFPortConfigSerializerVer11.toWireValue(Collections.singleton(OFPortConfig.PORT_DOWN));
			break;
		case OF_12:
			config = OFPortConfigSerializerVer12.toWireValue(Collections.singleton(OFPortConfig.PORT_DOWN));
			break;
		case OF_13:
			config = OFPortConfigSerializerVer13.toWireValue(Collections.singleton(OFPortConfig.PORT_DOWN));
			break;
		case OF_14:
			config = OFPortConfigSerializerVer14.toWireValue(Collections.singleton(OFPortConfig.PORT_DOWN));
			break;
		default:
			//log.error("Bad OFVersion {}", sw.getOFFactory().getVersion().toString());
			break;
		}
		return config;
	}
	
}

	