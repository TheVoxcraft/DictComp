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


/*

Compression spec:

[    2 bits   ] [     30 bits    ]  ...
{ control bit } { lookup address }  
                { / uncompressed }

Control bits:
00 - compressed
10 - reversed
01 - inverted
11 - uncompressed (30 bits)

    If uncompressed:

    Maximum six subsequent uncompressed packets. 24 bits, 4 (2/2) bits per 64 bit signature.
    
    11 after uncompressed means more uncompressed data.
    00 after uncompressed data means 2 crumbs (2 bits) are to be concatinated with uncompressed data.

Last 8 bytes are completely unaltered.

*/

public class DCCompress{

    static Map<Long, Integer> DCIndex; // {Key, Position}

    public static void main(String[] args) {
        System.out.println("DC Compressor v1.0");

        String relPath = "C:/Users/jonas/Dropbox/Prog Projects/DictComp/"+"/";

        String compressionIndexPath = relPath+"test_index.dc";
        String uncompressedFilePath = relPath+"testCompress/FP.LOG";

        if(args.length == 1){
            System.out.println("Using argument. {filename}"); 
            uncompressedFilePath = args[0];
        }

        if(args.length >= 2){
            System.out.println("Using arguments. {index filename}");
            compressionIndexPath = args[0];
            uncompressedFilePath = args[1];
        }

        loadDCIndex(compressionIndexPath);
        System.out.println("Loaded index: "+compressionIndexPath);

        System.out.println("Compressing "+uncompressedFilePath);

        byte[] rawFile = loadFile(uncompressedFilePath);
        int uncompressedFileSize = rawFile.length;
        System.out.println("Uncompressed size: "+uncompressedFileSize);

        byte[] compressedFile = ByteListToByteArray(compressFile(rawFile));
        printInfoOnCompression(compressedFile.length, uncompressedFileSize);

        //compressedFile = ByteListToByteArray(compressFile(compressedFile));
        //printInfoOnCompression(compressedFile.length, uncompressedFileSize);

        writeCompFileToDisk(compressedFile, uncompressedFilePath+".dc");


        //debug_WriteBinaryToFile(rawFile, relPath+"all bytes.log");
        //debug_IndexWriteBinaryToFile(compressionIndexPath, relPath+"all bytes_index.log");
    }

    static void printInfoOnCompression(int compSize, int unCompSize){
        System.out.println("Compressed size: "+compSize);
        float compRatio = Math.round(((float)compSize/(float)unCompSize)*1000)/10f;
        System.out.println("Compression ratio: "+compRatio+"%");
    }

    static List<Byte> compressFile(byte[] rawFile){
        List<Byte> Compressed = new ArrayList<>();
        List<Integer> LostBits = new ArrayList<>();

        int CountUncompressablePackets = 0; // Max 6
        boolean NoCompressionState = false;
        for (int i = 0; i < rawFile.length-8; i+=8) {
            byte[] cLong = new byte[8];
            for (int j = 0; j < 8; j++) { // Make long
                cLong[j] = rawFile[i+j];
            }
            long pattern = toLong(cLong);

            boolean foundNPattern = DCIndex.containsKey(pattern);
            boolean foundIPattern = DCIndex.containsKey(~pattern);
            boolean foundRPattern = DCIndex.containsKey(Long.reverse(pattern));

            // If Recovery Packet is full, or found new compressable pattern while in NoCompressionState
            if( CountUncompressablePackets >= 6 || (NoCompressionState && (foundNPattern || foundIPattern || foundRPattern))){
                //System.out.print("r"+CountUncompressablePackets);
                int recoveryPacket = makeLostBitsRecoveryPacket(LostBits); // create packet
                for (byte b : intToBytes(recoveryPacket)) { Compressed.add(b); } // add to compressed file
                // Reset
                CountUncompressablePackets = 0;
                NoCompressionState = false;
                LostBits.clear();
            }

            if(foundNPattern){ // Normal compression
                //System.out.print("C");
                int patternIndex = DCIndex.get(pattern);
                int signature = patternIndex & 0x3FFFFFFF; // set normal compression control bits
                //System.out.print("C<"+ Integer.toBinaryString(signature)+">");
                for (byte b : intToBytes(signature)) { Compressed.add(b); }

            } else if(foundIPattern){ // Inverted compression
                //System.out.print("I");
                int patternIndex = DCIndex.get(~pattern);
                int signature = patternIndex & 0x3FFFFFFF; // set default compression control bits
                signature = patternIndex | 0x40000000; // set inverted compression control bits
                for (byte b : intToBytes(signature)) { Compressed.add(b); }

            } else if(foundRPattern){ // Reversed compression
                //System.out.print("R");
                int patternIndex = DCIndex.get(Long.reverse(pattern));
                int signature = patternIndex & 0x3FFFFFFF; // set default compression control bits
                signature = patternIndex | 0x80000000; // set reversed compression control bits
                for (byte b : intToBytes(signature)) { Compressed.add(b); }

            } else { // No compression
                //System.out.print(".");
                int hi = (int) (pattern >> 32); // get high register from 64 bit (32)
                int lo = (int) pattern;         // get low register from 64 bit  (32)
                LostBits.add(hi & 0xC0000000);
                LostBits.add(lo & 0xC0000000);
                hi = hi & 0x3FFFFFFF; // remove highest 2 bit
                lo = lo & 0x3FFFFFFF; // remove highest 2 bit
                
                for (byte b : intToBytes(hi)) { Compressed.add(b); }
                for (byte b : intToBytes(lo)) { Compressed.add(b); }

                CountUncompressablePackets += 2;
                NoCompressionState = true;
            }
        }

        // Add last 8 bytes. Unaltered.
        for (int i = rawFile.length-8; i < rawFile.length; i++) { Compressed.add(rawFile[i]); }

        return Compressed;
    }

    static int makeLostBitsRecoveryPacket(List<Integer> LostBits){
        if(LostBits.size() > 6){ System.out.println("ERROR! >6"); }

        int packet = 0;
        for (int i = 0; i < LostBits.size(); i++) {
            int val = LostBits.get(i);
            int off = (2*i);
            packet |= (val >> off);
        }

        return packet;
    }

    static void visualizeCompression(byte[] rawFile){
        for (int i = 0; i < rawFile.length-8; i+=8) {
            byte[] cLong = new byte[8];
            for (int j = 0; j < 8; j++) { // Make long
                cLong[j] = rawFile[i+j];
            }
            long pattern = toLong(cLong);

            if(DCIndex.containsKey(pattern)){
                System.out.print("C");
            } else if(DCIndex.containsKey(~pattern)){
                System.out.print("I");
            } else if(DCIndex.containsKey(Long.reverse(pattern))){
                System.out.print("R");
            } else {
                System.out.print(".");
            }
        }
    }

    static void writeCompFileToDisk(byte[] file, String filename){
        Path path = Paths.get(filename);
        try {
            Files.write(path, file);
            System.out.println("Written index on path: "+filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static byte[] ByteListToByteArray(List<Byte> list){
        byte[] converted = new byte[list.size()];
        for (int i = 0; i < converted.length; i++) {
            converted[i] = list.get(i);
        }
        return converted;
    }

    static void debug_WriteBinaryToFile(byte[] rawFile, String path){
        try(FileWriter fw = new FileWriter(path, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            for (int i = 0; i < rawFile.length-8; i+=8) {
                byte[] cLong = new byte[8];
                for (int j = 0; j < 8; j++) { // Make long
                    cLong[j] = rawFile[i+j];
                }
                long pattern = toLong(cLong);
                out.println(Long.toBinaryString(pattern));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void debug_IndexWriteBinaryToFile(String ipath, String lpath){
        try(FileWriter fw = new FileWriter(lpath, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            byte[] allBytes;
            allBytes = Files.readAllBytes(Paths.get(ipath));

            DCIndex = new HashMap<>(allBytes.length);

            for (int b = 0; b < allBytes.length; b+=8) {
                byte[] eightBytes = new byte[8];
                for (int i = 0; i < 8; i++) {
                    eightBytes[i] = allBytes[b+i];
                }
                out.println(LongToBinString(toLong(eightBytes)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static byte[] loadFile(String path){
        byte[] allBytes = new byte[0];
        try {
            allBytes = Files.readAllBytes(Paths.get(path));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        if(allBytes.length < 8){
            System.out.println("Too few bytes!");
        }
        return allBytes;
    }

    static void loadDCIndex(String path){
        byte[] allBytes;
        try {
            allBytes = Files.readAllBytes(Paths.get(path));

            DCIndex = new HashMap<>(allBytes.length);

            int FilePosition = 0;
            for (int b = 0; b < allBytes.length; b+=8) {
                byte[] eightBytes = new byte[8];
                for (int i = 0; i < 8; i++) {
                    eightBytes[i] = allBytes[b+i];
                }
                DCIndex.put(toLong(eightBytes), FilePosition);
                FilePosition++;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
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

    static byte[] intToBytes( final int i ) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }

    static String LongToBinString(long i){
        return "0".repeat(Long.numberOfLeadingZeros(i != 0 ? i : 1)) + Long.toBinaryString(i);
    }

}