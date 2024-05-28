package master.pojo;

import lombok.Data;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

public class Region {
    public ArrayList<String> tables;
    public String ip;
    public String port;
    public String regionName;
    public String dbUserName;
    public String dbPassword;

    public Region() throws UnknownHostException {
        try{
            this.ip = InetAddress.getLocalHost().getHostAddress();
        }catch (UnknownHostException e){
            throw new RuntimeException(e);
        }
    }

    public Region(ArrayList<String> tables, String port, String regionName) throws UnknownHostException {
        try{
            this.ip = InetAddress.getLocalHost().getHostAddress();
            this.tables = tables;
            this.port = port;
            this.regionName = regionName;
        }catch (UnknownHostException e){
            throw new RuntimeException(e);
        }
    }

    public String toZKNodeValue(){
        StringBuilder string = new StringBuilder();
        string.append(ip).append(",").append(port).append(",").append(regionName).append(",").append(dbUserName).append(",").append(dbPassword).append(",").append(tables.size()).append(",");
        for(String table:tables){
            string.append(table).append(",");
        }
        return string.substring(0, string.length()-1);
    }

    public void deserializeFromString(String regionInfo){
        String[] strings = regionInfo.split(",");
        this.ip = strings[0];
        this.port = strings[1];
        this.regionName = strings[2];
        this.dbUserName = strings[3];
        this.dbPassword = strings[4];
        int length = Integer.parseInt(strings[5]);
        this.tables = new ArrayList<String>();
        this.tables.addAll(Arrays.asList(strings).subList(6, length + 6));
    }

    public boolean isSlaveNode(String tableName){
        return tables.contains(tableName + "_slave");
    }

    public boolean containsTable(String tableName){
        return tables.contains(tableName);
    }

    public ArrayList<String> getTables() {
        return tables;
    }

    public String getIp() {
        return ip;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getPort() {
        return port;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setTables(ArrayList<String> tables) {
        this.tables = tables;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getDbUserName() {
        return dbUserName;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public void setDbUserName(String dbUserName) {
        this.dbUserName = dbUserName;
    }
}
