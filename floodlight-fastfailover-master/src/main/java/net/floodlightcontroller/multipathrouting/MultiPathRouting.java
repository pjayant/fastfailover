package net.floodlightcontroller.multipathrouting;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.List;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.LinkedList;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.NodePortTuple;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.routing.RouteId;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.multipathrouting.types.LinkWithCost;
import net.floodlightcontroller.multipathrouting.types.MultiRoute;
import net.floodlightcontroller.multipathrouting.types.NodeCost;
import net.floodlightcontroller.multipathrouting.types.FlowId;

import net.floodlightcontroller.restserver.IRestApiService;

import org.projectfloodlight.openflow.util.HexString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;

public class MultiPathRouting implements IFloodlightModule ,ITopologyListener, IMultiPathRoutingService{
    protected static Logger logger;
    protected IFloodlightProviderService floodlightProvider;
    protected ITopologyService topologyService;
    protected IRestApiService restApi;

    protected final int ROUTE_LIMITATION = 10;
    protected HashMap<DatapathId, HashSet<LinkWithCost>> dpidLinks;
    protected int pathCount = 0;

    protected class FlowCacheLoader extends CacheLoader<FlowId,Route> {
        MultiPathRouting mpr;
        FlowCacheLoader(MultiPathRouting mpr) {
            this.mpr = mpr;
        }

        @Override
        public Route load(FlowId fid) {
            return mpr.buildFlowRoute(fid);
        }
    }

    private final FlowCacheLoader flowCacheLoader = new FlowCacheLoader(this);
    protected LoadingCache<FlowId,Route> flowcache;

    protected class PathCacheLoader extends CacheLoader<RouteId,MultiRoute> {
        MultiPathRouting mpr;
        PathCacheLoader(MultiPathRouting mpr) {
            this.mpr = mpr;
        }

        @Override
        public MultiRoute load(RouteId rid) {
            return mpr.buildMultiRoute(rid);
        }
    }
    private final PathCacheLoader pathCacheLoader = new PathCacheLoader(this);
    protected LoadingCache<RouteId,MultiRoute> pathcache;

    //
    //
    //ITopologyListener
    //
    //
    @Override
    public void topologyChanged(List<LDUpdate> linkUpdates) {
        for (LDUpdate update : linkUpdates) {
            if (update.getOperation().equals(ILinkDiscovery.UpdateOperation.LINK_REMOVED) || update.getOperation().equals(ILinkDiscovery.UpdateOperation.LINK_UPDATED)) {
                LinkWithCost srcLink = new LinkWithCost(update.getSrc(), update.getSrcPort(), update.getDst(), update.getDstPort(),1);
                LinkWithCost dstLink = srcLink.getInverse();

                if (update.getOperation().equals(ILinkDiscovery.UpdateOperation.LINK_REMOVED)) {
                    removeLink(srcLink);
                    removeLink(dstLink);
                    clearRoutingCache();
                } else if (update.getOperation().equals(ILinkDiscovery.UpdateOperation.LINK_UPDATED)) {
                    addLink(srcLink);
                    addLink(dstLink);
                    clearRoutingCache();
                }
            }
        }

    }
    public void clearRoutingCache() {
         flowcache.invalidateAll();
         pathcache.invalidateAll();
    }
    public void removeLink(LinkWithCost link) {
        DatapathId dpid = link.getSrcDpid();

        if (null == dpidLinks.get(dpid)) {
            return;
        }

		dpidLinks.get(dpid).remove(link);
        if (0 == dpidLinks.get(dpid).size())
       		dpidLinks.remove(dpid);
    }
    public void addLink(LinkWithCost link) {
        DatapathId dpid = link.getSrcDpid();

        if (null == dpidLinks.get(dpid)) {
            HashSet<LinkWithCost> links = new HashSet<LinkWithCost>();
            links.add(link);
            dpidLinks.put(dpid,links);
        } else {
            dpidLinks.get(dpid).add(link);
        }
    }
    public Route buildFlowRoute(FlowId fid) {
        DatapathId srcDpid = fid.getSrc();
        DatapathId dstDpid = fid.getDst();
        OFPort srcPort = fid.getSrcPort();
        OFPort dstPort = fid.getDstPort();

        List<NodePortTuple> nptList;
        NodePortTuple npt;
        MultiRoute routes = null;
        Route result = null;

        try {
            routes = pathcache.get(new RouteId(srcDpid,dstDpid));
        } catch (Exception e) {
            logger.error("error {}",e.toString());
        }

        if (0 == routes.getRouteSize()) {
            result = null;
		} else {
            result = routes.getRoute();
		}

        if (result != null) {
            nptList= new ArrayList<NodePortTuple>(result.getPath());
        } else {
            nptList = new ArrayList<NodePortTuple>();
        }

        npt = new NodePortTuple(srcDpid, srcPort);
        nptList.add(0, npt);
        npt = new NodePortTuple(dstDpid, dstPort);
        nptList.add(npt);

        result = new Route(new RouteId(srcDpid,dstDpid), nptList);
        return result;
    }

    public MultiRoute buildMultiRoute(RouteId rid) {
        return computeMultiPath(rid);
    }

    public MultiRoute computeMultiPath(RouteId rid) {
        DatapathId srcDpid = rid.getSrc();
        DatapathId dstDpid = rid.getDst();
        MultiRoute routes = new MultiRoute();
        
        HashMap<DatapathId, HashSet<LinkWithCost>> links = new HashMap<DatapathId, HashSet<LinkWithCost>>();
        //keep a deep cloned copy because we will change the costs in calculation but don't want to affect the real ones.
        for(DatapathId dpid : dpidLinks.keySet()){
        	links.put(dpid, dpidLinks.get(dpid));
        }

        if (srcDpid == dstDpid) {
            return routes;
		}

        if (null == dpidLinks.get(srcDpid) || null == dpidLinks.get(dstDpid)) {
            return routes;
		}
        pathCount = 0;
        
        Route route1 = buildShortestPath(srcDpid, dstDpid, links);
        //System.out.print("Route: "+route1.toString()+".\n");
        routes.addRoute(route1);
        HashMap<DatapathId, HashSet<LinkWithCost>> modifiedLinks = neglectLinks(links, route1);
        Route backupRoute = buildShortestPath(srcDpid, dstDpid, modifiedLinks);
        //System.out.print("BackupRoute: "+backupRoute.toString()+".\n");
        routes.addRoute(backupRoute);
        ////Do again. Just in case.
        //HashMap<DatapathId, HashSet<LinkWithCost>> modifiedLinks2 = neglectLinks(links, backupRoute);
        //Route backupRoute2 = buildShortestPath(srcDpid, dstDpid, modifiedLinks2);
        //System.out.print("BackupRoute: "+backupRoute.toString()+".\n");
        //routes.addRoute(backupRoute2);
        
        return routes;
    }
    
    public Route buildShortestPath(DatapathId srcDpid, DatapathId dstDpid, HashMap<DatapathId, HashSet<LinkWithCost>> links) {
    	HashMap<DatapathId, LinkWithCost> previous = new HashMap<DatapathId, LinkWithCost>();
    	previous = runDijkstra(srcDpid, dstDpid, links);
    	Route result = generateRoute(srcDpid, dstDpid, previous);
    	return result;
    }
    
    public HashMap<DatapathId, LinkWithCost> runDijkstra(DatapathId srcDpid, DatapathId dstDpid, HashMap<DatapathId, HashSet<LinkWithCost>> links){
    	HashMap<DatapathId, LinkWithCost> previous = new HashMap<DatapathId, LinkWithCost>();
    	HashMap<DatapathId, Integer> costs = new HashMap<DatapathId, Integer>();
        for(DatapathId dpid : links.keySet()){
            costs.put(dpid,Integer.MAX_VALUE);
        }
        PriorityQueue<NodeCost> nodeq = new PriorityQueue<NodeCost>();
        HashSet<DatapathId> seen = new HashSet<DatapathId>();
        nodeq.add(new NodeCost(srcDpid,0));
        NodeCost node;
        while( null != nodeq.peek()){
            node = nodeq.poll();
            if(node.getDpid() ==  dstDpid)
                break;
            int cost = node.getCost();
            seen.add(node.getDpid());
            for (LinkWithCost link: links.get(node.getDpid())) {
                DatapathId dst = link.getDstDpid();
                int totalCost = link.getCost() + cost;
                if( true == seen.contains(dst))
                    continue;

                if( totalCost < costs.get(dst) ){
                    costs.put(dst,totalCost);
                    previous.put(dst, link.getInverse());

                    NodeCost ndTemp = new NodeCost(dst,totalCost);
                    nodeq.remove(ndTemp);
                    nodeq.add(ndTemp);
                }
            }
        }
        return previous;
    }
    
    public Route generateRoute(DatapathId srcDpid, DatapathId dstDpid, HashMap<DatapathId, LinkWithCost> previous){
    	DatapathId current = dstDpid;
    	LinkedList<NodePortTuple> switchPorts = new LinkedList<NodePortTuple>();
    	
    	while( current != srcDpid){
    		LinkWithCost link = previous.get(current);
    		NodePortTuple npt = new NodePortTuple(link.getDstDpid(), link.getDstPort());
            NodePortTuple npt2 = new NodePortTuple(link.getSrcDpid(), link.getSrcPort());
            switchPorts.addFirst(npt2);
            switchPorts.addFirst(npt);
            current = link.getDstDpid();
            //System.out.print("Processing tuple "+npt.toString()+".\n");
    	}
    	
    	pathCount++;
        Route result = new Route(new RouteId(srcDpid,dstDpid), new LinkedList<NodePortTuple>(switchPorts));
        return result;
    }
    
    private HashMap<DatapathId, HashSet<LinkWithCost>> neglectLinks(HashMap<DatapathId, HashSet<LinkWithCost>> links, Route route1) {
    	if(null != route1) {
    		List<NodePortTuple> switchPorts = route1.getPath();
    		for (int idx = switchPorts.size() - 1; idx > 0; idx -= 2) {
    			// indx and indx-1 will always have the same switch DPID.
    			DatapathId dstDpid = switchPorts.get(idx).getNodeId();
    			DatapathId srcDpid = switchPorts.get(idx-1).getNodeId();
    			for(LinkWithCost link: links.get(srcDpid)) {
                    if (link.getSrcDpid() == srcDpid && link.getDstDpid() == dstDpid) {
                        link.setCost(10);
                    }
    			}
    		}
    	}
    	return links;
    }
    
/*    public void generateMultiPath(MultiRoute routes, DatapathId srcDpid, DatapathId dstDpid, DatapathId current, HashMap<DatapathId, HashSet<LinkWithCost>> previous,LinkedList<NodePortTuple> switchPorts)
    {   if (pathCount >=ROUTE_LIMITATION) {
            return ;
		}

        if (current == srcDpid) {
            pathCount++;
            Route result = new Route(new RouteId(srcDpid,dstDpid), new LinkedList<NodePortTuple>(switchPorts));
            routes.addRoute(result);
            return ;
        }

        HashSet<LinkWithCost> links = previous.get(current);
        for(LinkWithCost link: links) {
            NodePortTuple npt = new NodePortTuple(link.getDstDpid(), link.getDstPort());
            NodePortTuple npt2 = new NodePortTuple(link.getSrcDpid(), link.getSrcPort());
            switchPorts.addFirst(npt2);
            switchPorts.addFirst(npt);
            generateMultiPath(routes,srcDpid, dstDpid, link.getDstDpid(), previous,switchPorts);
            switchPorts.removeFirst();
            switchPorts.removeFirst();

        }
        return ;
    }*/

    private void updateLinkCost(DatapathId srcDpid,DatapathId dstDpid,int cost) {
        if (null != dpidLinks.get(srcDpid)) {
            for(LinkWithCost link: dpidLinks.get(srcDpid)) {
                if (link.getSrcDpid() == srcDpid && link.getDstDpid() == dstDpid) {
                    link.setCost(cost);
                    return;
                }
            }
        }
    }

    //
    //IMultiPathRoutingService implement
    //
    //
    @Override
    public Route getRoute(DatapathId srcDpid,OFPort srcPort,DatapathId dstDpid,OFPort dstPort) {
        // Return null the route source and desitnation are the
        // same switchports.
        if (srcDpid == dstDpid && srcPort == dstPort) {
            return null;
		}

        FlowId id = new FlowId(srcDpid,srcPort,dstDpid,dstPort);
        Route result = null;

        try {
            result = flowcache.get(id);
        } catch (Exception e) {
            logger.error("error {}",e.toString());
        }

        if (result == null && srcDpid != dstDpid) {
			return null;
		}

        return result;
    }
    
    @Override
    public Route getBackupRoute(Route route1){
    	DatapathId srcDpid = route1.getId().getSrc();
    	DatapathId dstDpid = route1.getId().getDst();
    	NodePortTuple ingress = route1.getPath().get(0);
    	NodePortTuple egress = route1.getPath().get(route1.getPath().size() -1);
    ///System.out.println("Inside getBAckupRoute function..." + route1.toString() +" ");
    	List<NodePortTuple> nptList;
    	Route backupRoute = null;
    	// call getRoute again to get shortest route
        MultiRoute multiRoute = getMultiRoute(srcDpid,dstDpid);
        //System.out.println("The whole multiroute array: "+multiRoute.getRoutes().toString());
        //System.out.println("The route count value " +multiRoute.getRouteCount());
		 backupRoute = multiRoute.getRoute();
		
		//while(backupRoute == route1){
			// call again when the first returned route is in use.
			//backupRoute = multiRoute.getRoute();
			//System.out.println("------Log------ Route in use, get another one.");
		//}
		
		if (backupRoute != null) {
			nptList= new ArrayList<NodePortTuple>(backupRoute.getPath());
        } else {
        	nptList = new ArrayList<NodePortTuple>();
        }
		
		NodePortTuple npt = new NodePortTuple(ingress.getNodeId(), ingress.getPortId());
        nptList.add(0, npt);
        npt = new NodePortTuple(egress.getNodeId(), egress.getPortId());
        nptList.add(npt);

        backupRoute = new Route(new RouteId(srcDpid,dstDpid), nptList);
        
        //System.out.println("!!!!Route In Use!!!! "+route1.toString());
       
        while(backupRoute.equals(route1))
        {
        	System.out.println("###############While Loop Running############");
        	backupRoute = multiRoute.getRoute();
        	nptList= new ArrayList<NodePortTuple>(backupRoute.getPath());
        	npt = new NodePortTuple(ingress.getNodeId(), ingress.getPortId());
            nptList.add(0, npt);
            npt = new NodePortTuple(egress.getNodeId(), egress.getPortId());
            nptList.add(npt);
            backupRoute = new Route(new RouteId(srcDpid,dstDpid), nptList);
        }	
		return backupRoute;
    }

	@Override
	public MultiRoute getMultiRoute(DatapathId srcDpid, DatapathId dstDpid) {

		if (srcDpid == dstDpid) {
			return null;
		}

		RouteId rId = new RouteId(srcDpid, dstDpid);
		MultiRoute result = null;

		try {
			result = pathcache.get(rId);
		} catch (Exception e) {
			logger.error("error {}", e.toString());
		}

		return result;
	}

    @Override
    public void modifyLinkCost(DatapathId srcDpid,DatapathId dstDpid,short cost) {
        updateLinkCost(srcDpid,dstDpid,cost);
        updateLinkCost(dstDpid,srcDpid,cost);
        clearRoutingCache();
    }


    //
    //
    //IFloodlightModule
    //
    //
    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        Collection<Class<? extends IFloodlightService>> l =
                new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IMultiPathRoutingService.class);
        return l;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        Map<Class<? extends IFloodlightService>,
        IFloodlightService> m =
            new HashMap<Class<? extends IFloodlightService>,
                IFloodlightService>();
        m.put(IMultiPathRoutingService.class, this);
        return m;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        Collection<Class<? extends IFloodlightService>> l =
        new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IFloodlightProviderService.class);
        l.add(ITopologyService.class);
//      l.add(IRestApiService.class);
        return l;
    }

    @Override
    public void init(FloodlightModuleContext context)
            throws FloodlightModuleException {
        floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
        topologyService    = context.getServiceImpl(ITopologyService.class);
        restApi = context.getServiceImpl(IRestApiService.class);
        logger = LoggerFactory.getLogger(MultiPathRouting.class);
        dpidLinks = new HashMap<DatapathId, HashSet<LinkWithCost>>();

        flowcache = CacheBuilder.newBuilder().concurrencyLevel(4)
                    .maximumSize(1000L)
                    .build(
                            new CacheLoader<FlowId,Route>() {
                                public Route load(FlowId fid) {
                                    return flowCacheLoader.load(fid);
                                }
                           });
        pathcache = CacheBuilder.newBuilder().concurrencyLevel(4)
                    .maximumSize(1000L)
                    .build(
                            new CacheLoader<RouteId,MultiRoute>() {
                                public MultiRoute load(RouteId rid) {
                                    return pathCacheLoader.load(rid);
                                }
                            });
    }

    @Override
    public void startUp(FloodlightModuleContext context) {
        topologyService.addListener(this);
        //restApi.addRestletRoutable(new MultiPathRoutingWebRoutable());
    }
}
