import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.stream.*;
import java.io.*;
import java.nio.file.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import io.timeandspace.smoothie.*;

public class DCIndexGenerator{

    final static int PATTERN_FIND_JUMP = 8; // DEFAULT/MAX IS '8' BITS

    static int countUpdates = 0;
    static int countSpecial = 0;
    static Map<Long, Integer> myMap = SmoothieMap.<Long, Integer>newBuilder().build();
    static Map<Long, Integer> CompIndex = new HashMap<>();  // {Key, Position}

    public static void main(String[] args){
        //myMap = SmoothieMap.<Long, Integer>newBuilder().build();
        //SmoothieMapBuilder<Long, Integer> builder = SmoothieMap<>.newBuilder();
        //CompIndex =  builder.expectedSize(78_900_000).build();

        System.out.println("Max mem: "+Runtime.getRuntime().maxMemory());
        
        String relPath = "";
        String computeFolderFilename = "G:/Games/Superfighters Deluxe v1.3.7b/";
        String DCIndexFilename = "C:/Users/jonas/Dropbox/Prog Projects/DictComp/test_index.dc";

        computeFolderWithAllOptimize(relPath+computeFolderFilename, 20000);

        writeDCIndexToFile(relPath+DCIndexFilename);

        System.out.println("Optimized: "+countSpecial);
        System.out.println("Size: "+CompIndex.size()+" / "+countUpdates);
    }

    static void updateIndex(long bits){
        countUpdates++;
        /* Debug: print raw bytes
        String b = new String(longToBytes(bits), StandardCharsets.UTF_8);
        System.out.println(LongToBinString(bits)+" "+LongToBinString(bits).length()+": "+b);
        */

        if(CompIndex.containsKey(bits)){
            CompIndex.put(bits, CompIndex.get(bits) + 1);
        } else { // New key
            CompIndex.put(bits, 1);
        }
    }

    static void updateReversedIndex(long bits){
        long rbits = Long.reverse(bits);
        if(CompIndex.containsKey(rbits)){
            countSpecial++;
            CompIndex.put(rbits, CompIndex.get(bits) + 1);
        }
    }

    static void updateInvertedIndex(long bits){
        long ibits = ~bits;
        if(CompIndex.containsKey(ibits)){
            countSpecial++;
            CompIndex.put(ibits, CompIndex.get(bits) + 1);
        }
    }

    static void computeFile(String path){
        byte[] allBytes;
        try {
            allBytes = Files.readAllBytes(Paths.get(path));
            for (int b = 0; b < allBytes.length-8; b+=PATTERN_FIND_JUMP) {
                byte[] eightBytes = new byte[8];
                for (int i = 0; i < 8; i++) {
                    eightBytes[i] = allBytes[b+i];
                }
                long concat = toLong(eightBytes);
                updateIndex(concat);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    static void computeFileReversed(String path){
        byte[] allBytes;
        try {
            allBytes = Files.readAllBytes(Paths.get(path));
            for (int b = 0; b < allBytes.length-8; b+=PATTERN_FIND_JUMP) {
                byte[] eightBytes = new byte[8];
                for (int i = 0; i < 8; i++) {
                    eightBytes[i] = allBytes[b+i];
                }
                long concat = toLong(eightBytes);
                updateReversedIndex(concat);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    static void computeFileInverted(String path){
        byte[] allBytes;
        try {
            allBytes = Files.readAllBytes(Paths.get(path));
            for (int b = 0; b < allBytes.length-8; b+=PATTERN_FIND_JUMP) {
                byte[] eightBytes = new byte[8];
                for (int i = 0; i < 8; i++) {
                    eightBytes[i] = allBytes[b+i];
                }
                long concat = toLong(eightBytes);
                updateInvertedIndex(concat);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    static void computeFolder(String path, CompressionType cType){
        int CountFiles = 0;
        try (Stream<Path> walk = Files.walk(Paths.get(path))) {

            List<String> files = walk.filter(Files::isRegularFile)
                    .map(x -> x.toString()).collect(Collectors.toList());
    
            if(files == null){
                System.out.println("No files found!");
                return;
            }
            for (String file : files) {
                switch (cType){
                    case NORMAL:
                        computeFile(file);
                        break;
                    case INVERTED:
                        computeFileInverted(file);
                        break;
                    case REVERSED:
                        computeFileReversed(file);
                        break;
                }

                if(CountFiles % 384 == 0){
                    System.out.println("Computing index... "+CountFiles+"/"+files.size()+" - "+files.get(CountFiles));
                }
                CountFiles++;
            }

            System.out.println("Number of files: "+CountFiles);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void computeFolderWithAllOptimize(String path, int minimumSize){
        System.out.println("Starting hash index creation on "+path);

        computeFolder(path, CompressionType.NORMAL);
        System.out.println("Created hashmap for all 64 bit patterns. Best score: "+getBestScore());

        computeFolder(path, CompressionType.REVERSED);
        System.out.println("Optimized hashmap (reversed). Best score: "+getBestScore());

        computeFolder(path, CompressionType.INVERTED);
        System.out.println("Optimized hashmap (inverted). Best score: "+getBestScore());

        //int r = removeLowScorePatterns(minimumSize);
        //System.out.println("Removed low score patterns. Removed: "+r);
    }

    static int removeLowScorePatterns(int minimumSize){
        List<Long> toRemove = new ArrayList<>();
        for(long key : CompIndex.keySet()){
            if(CompIndex.get(key) < minimumSize){
                toRemove.add(key);
            }
        }
        for(long key : toRemove){
            CompIndex.remove(key);
        }
        return toRemove.size();
    }

    static int getBestScore(){
        int bestScore = 0;
        long bestKey = 0;
        for(long key : CompIndex.keySet()){
            int score = CompIndex.get(key);
            if(score > bestScore){
                bestScore = score;
                bestKey = key;
            }
        }
        System.out.println("Best key: "+Long.toHexString(bestKey));
        return bestScore;
    }

    static float getAvgScore(){
        int totalScore = 0;
        for(long key : CompIndex.keySet()){
            totalScore += CompIndex.get(key);
        }
        return totalScore;
    }

    public static long toLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getLong();
    }

    static byte[] longToBytes( final long i ) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(i);
        return bb.array();
    }

    static ArrayList<Long> getSortedDCIndex(){
        LinkedHashMap<Long, Integer> sortedMap = CompIndex.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (x,y)-> {throw new AssertionError();},
                        LinkedHashMap::new
                ));
        return new ArrayList(sortedMap.keySet());
    }

    static byte[] SerializeHashIndex(){
        ArrayList<Long> keys = getSortedDCIndex();
        System.out.println("Sorted by score.");
        byte[] serialized = new byte[keys.size()*8];
        for (int i = 0; i < keys.size(); i++) {
            byte[] bytes = longToBytes(keys.get(i));
            for (int j = 0; j < 8; j++) {
                serialized[(i*8)+j] = bytes[j];
            }
        }
        return serialized;
    }

    static void writeDCIndexToFile(String filename){

        byte[] SortedSerialized = SerializeHashIndex();

        Path path = Paths.get(filename);
        try {
            Files.write(path, SortedSerialized);
            System.out.println("Written index on path: "+filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String LongToBinString(long i){
        return "0".repeat(Long.numberOfLeadingZeros(i != 0 ? i : 1)) + Long.toBinaryString(i);
    }

    enum CompressionType {
        NORMAL,
        INVERTED,
        REVERSED
    }
}

