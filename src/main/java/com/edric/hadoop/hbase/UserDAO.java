package com.edric.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by edric on 9/10/15.
 */
public class UserDAO {
    public static final byte[] TABLE_NAME = Bytes.toBytes("users");
    public static final byte[] INFO_FAM = Bytes.toBytes("info");
    public static final byte[] ID_COL = Bytes.toBytes("id");
    public static final byte[] NAME_COL = Bytes.toBytes("name");
    public static final byte[] EMAIL_COL = Bytes.toBytes("email");
    public static final byte[] PASS_COL = Bytes.toBytes("password");

    private Connection conn;

    public UserDAO(Connection conn) {
        this.conn = conn;
    }

    private static Get mkGet(String userId) {
        Get g = new Get(Bytes.toBytes(userId));
        g.addFamily(INFO_FAM);
        return g;
    }

    private static Put mkPut(User user) {
        Put p = new Put(Bytes.toBytes(user.id));
        p.addColumn(INFO_FAM, NAME_COL, Bytes.toBytes(user.name));
        p.addColumn(INFO_FAM, EMAIL_COL, Bytes.toBytes(user.email));
        p.addColumn(INFO_FAM, PASS_COL, Bytes.toBytes(user.password));
        return p;
    }

    private static Delete mkDel(String userId) {
        Delete d = new Delete(Bytes.toBytes(userId));
        return d;
    }

    public void addUser(User user) {
        try  {
            Table users = conn.getTable(TableName.valueOf(TABLE_NAME));

            Put p = mkPut(user);
            users.put(p);
        } catch (IOException ioe) {

        }
    }

    public User getUser(String userId) {
        User user = new User();
        try  {
            Table users = conn.getTable(TableName.valueOf(TABLE_NAME));

            Get g = mkGet(userId);
            Result result = users.get(g);

            user.id = Bytes.toString(result.getRow());
            user.name = Bytes.toString(result.getValue(INFO_FAM, NAME_COL));
            user.email = Bytes.toString(result.getValue(INFO_FAM, EMAIL_COL));
            user.password = Bytes.toString(result.getValue(INFO_FAM, PASS_COL));

        } catch (IOException ioe) {

        }

        return user;
    }

    public void delUser(String userId) {
        try  {
            Table users = conn.getTable(TableName.valueOf(TABLE_NAME));

            Delete d = mkDel(userId);
            users.delete(d);

        } catch (IOException ioe) {

        }
    }

    public static void main(String[] args) throws IOException {
        Configuration myConf = HBaseConfiguration.create();
        myConf.set("hbase.zookeeper.quorum", "172.16.98.201,172.16.98.202,172.16.98.203,172.16.98.204,172.16.98.205");
        myConf.set("hbase.zookeeper.property.clientPort", "2181");

        User user = new User();
        user.id = "1";
        user.name = "edric";
        user.email = "edric@yanix.com";
        user.password = "test";

        UserDAO dao = new UserDAO(ConnectionFactory.createConnection(myConf));
        //dao.addUser(user);
        //User newUser = dao.getUser(user.id);
        //System.out.println(newUser);

        dao.delUser(user.id);
    }
}
