package org.opentripplanner.updater.mamba;

import com.beust.jcommander.internal.Maps;
import io.opentraffic.engine.data.pbf.ExchangeFormat;
import com.fasterxml.jackson.databind.JsonNode;

import org.opentripplanner.routing.edgetype.StreetEdge;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.traffic.Segment;
//import org.opentripplanner.traffic.SegmentSpeedSample;
//import org.opentripplanner.traffic.StreetSpeedSnapshot;
//import org.opentripplanner.traffic.StreetSpeedSnapshotSource;
import org.opentripplanner.updater.GraphUpdaterManager;
import org.opentripplanner.updater.PollingGraphUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.List;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.LongStream;

/**
 * Update the graph with traffic data from OpenTraffic.
 */
public class MambaTrafficUpdater extends PollingGraphUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MambaTrafficUpdater.class);

    private Graph graph;
    private GraphUpdaterManager graphUpdaterManager;

    /** the tile directory to search through */
    private File speedCsvFile;

    private boolean hasAlreadyRun = false;

    @Override
    protected void runPolling() throws Exception {
        LOG.info("Loading speed data");

        // Build a speed index now while we're running in our own thread. We'll swap it out
        // at the appropriate time with a GraphWriterRunnable, but no need to synchronize yet.
        /*
        // search through the tile directory
        for (File z : tileDirectory.listFiles()) {
            for (File x : z.listFiles()) {
                for (File y : x.listFiles()) {
                    if (!y.getName().endsWith(".traffic.pbf")) {
                        LOG.warn("Skipping non-traffic file {} in tile directory", y);
                        continue;
                    }

                    // Deserialize it
                    InputStream in = new BufferedInputStream(new FileInputStream(y));
                    ExchangeFormat.BaselineTile tile = ExchangeFormat.BaselineTile.parseFrom(in);
                    in.close();

                    // TODO: handle metadata

                    for (int i = 0; i < tile.getSegmentsCount(); i++) {
                        ExchangeFormat.BaselineStats stats = tile.getSegments(i);
                        SegmentSpeedSample sample;
                        try {
                            sample = new SegmentSpeedSample(stats);
                        } catch (IllegalArgumentException e) {
                            continue;
                        }
                        Segment segment = new Segment(stats.getSegment());
                        speedIndex.put(segment, sample);
                    }
                }
            }
        }

        LOG.info("Indexed {} speed samples", speedIndex.size());
        */
        
        //long[] csvEdgesToUpdate = {24784762,27460631,3,4};
        //float[] csvSpeeds = {24784762,27460631,3,4};
        java.util.List<Long> csvEdgesToUpdate = new ArrayList<Long>();
        java.util.List<Float> csvSpeeds = new ArrayList<Float>();
        
        LOG.info("Scanner");
        Scanner scanner = new Scanner(speedCsvFile);
        scanner.useDelimiter(",");
        while(scanner.hasNextLine()){
        	String line = scanner.nextLine();
            String[] fields = line.split(",");
            csvEdgesToUpdate.add(Long.valueOf(fields[0]));
        	csvSpeeds.add(Float.valueOf(fields[1]));
        }
        scanner.close();
        //Map<long,float> edgeUpdates = new Map<long,float>();
        int counter=0;
        for (StreetEdge se: graph.getStreetEdges()) {
        	long edgeId = se.wayId;
            boolean contained = csvEdgesToUpdate.stream().anyMatch(x -> x == edgeId);
            if(contained) {
            	int ind = csvEdgesToUpdate.indexOf(edgeId);
            	se.setCarSpeed(csvSpeeds.get(ind));
            	counter++;
            }
            
        }
        LOG.info("MAMBAUpdater: Updated "+counter+" segments");
        
//        graphUpdaterManager.execute(graph -> {
//            graph.streetSpeedSource.setSnapshot(new StreetSpeedSnapshot(speedIndex));
//        });
    }

    @Override
    protected void configurePolling(Graph graph, JsonNode config) throws Exception {
        this.graph = graph;
        speedCsvFile = new File(config.get("speedCsvFile").asText());
    }

    @Override
    public void setGraphUpdaterManager(GraphUpdaterManager updaterManager) {
        updaterManager.addUpdater(this);
        this.graphUpdaterManager = updaterManager;
    }

    @Override
    public void setup() throws Exception {
        /*graphUpdaterManager.execute(graph -> {
            graph.streetSpeedSource = new StreetSpeedSnapshotSource();
        });*/
    }

    @Override
    public void teardown() {
        graphUpdaterManager.execute(graph -> {
            graph.streetSpeedSource = null;
        });
    }
}
