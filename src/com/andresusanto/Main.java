package com.andresusanto;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        String command;
        Cluster cluster;
        Session session;
        Scanner scn = new Scanner(System.in);

        cluster = Cluster.builder().addContactPoint("167.205.35.19").build();
        session = cluster.connect("andresus");

        //ResultSet results = session.execute("SELECT * FROM users");
        //for (Row row : results) {
        //    System.out.format("%s %d\n", row.getString("username"), row.getString("password"));
        //}

        System.out.println("Welcome to CassandraTweet. (c) 2015 by Andre Susanto 13512028");
        System.out.println("Available commands:");
        System.out.println("\t /reg <username> <password>");
        System.out.println("\t /follow <follower> <user>");
        System.out.println("\t /tweet <username> <what to tweet>");
        System.out.println("\t /duser <username>");
        System.out.println("\t /dtimeline <username>");
        System.out.println("\t /exit");


        while( !(command = scn.next()).equals("/exit") ) {

            if (command.equals("/reg")){
                Exception ex = null;
                String username = scn.next();
                String password = scn.next();
                do {
                    ex = null;
                    try {
                        session.execute("INSERT INTO users (username, password) VALUES ('" + username + "', '" + password + "')");
                        System.out.println("Registration success!");
                    } catch (Exception x) {
                        ex = x;
                    }
                }while (ex != null);
            }else if (command.equals("/follow")){
                Exception ex = null;
                String follower = scn.next();
                String user = scn.next();
                do {
                    ex = null;
                    try {
                        session.execute("INSERT INTO followers (username, follower, since) VALUES ('" + user + "', '" + follower + "', " + new Date().getTime() + ")");
                    } catch (Exception x) {
                        ex = x;
                    }
                }while (ex != null);

                do {
                    ex = null;
                    try {
                        session.execute("INSERT INTO friends (username, friend, since) VALUES ('" + user + "', '" + follower + "', " + new Date().getTime() + ")");
                        //System.out.println("Follow success!");
                    } catch (Exception x) {
                        ex = x;
                    }
                }while (ex != null);

                System.out.println("Follow success!");

            }else if (command.equals("/tweet")){
                Exception ex = null;
                String user = scn.next();
                String tweet = scn.next();
                do {
                    ex = null;
                    try {
                        ResultSet results = session.execute("INSERT INTO tweets (tweet_id, username, body) VALUES ( now() ,'" + user + "', '" + tweet + "')");
                        System.out.println("Tweet success!");
                    } catch (Exception x) {
                        ex = x;
                    }
                }while (ex != null);
            }else if (command.equals("/duser")){
                Exception ex = null;
                String user = scn.next();
                do {
                    ex = null;
                    try {
                        ResultSet results = session.execute("SELECT * FROM tweets");
                        System.out.println(" -- tweets by " + user + " --");
                        for (Row row : results) {
                            if (row.getString("username").equals(user))
                                System.out.println(row.getString("username") + " -> " + row.getString("body"));
                        }
                        System.out.println(" --- ");
                    } catch (Exception x) {
                        ex = x;
                    }
                }while (ex != null);
            }else if (command.equals("/dtimeline")){
                String user = scn.next();
                List<String> following = new LinkedList<String>();

                Exception ex = null;

                do {
                    ex = null;
                    try {
                        ResultSet results = session.execute("SELECT * FROM followers");
                        for (Row row : results) {
                            if (row.getString("follower").equals(user))
                                following.add(row.getString("username"));
                        }

                    } catch (Exception x) {
                        ex = x;
                    }
                }while (ex != null);

                do {
                    ex = null;
                    try {
                        ResultSet resultz = session.execute("SELECT * FROM tweets");
                        System.out.println(" -- tweets in " + user + "'s timeline --");
                        for (Row row : resultz) {
                            if (following.contains(row.getString("username")) || row.getString("username").equals(user))
                                System.out.println(row.getString("username") + " -> " + row.getString("body"));
                        }
                        System.out.println(" --- ");
                    }catch(Exception x){
                        ex = x;
                    }
                }while (ex != null);



            }else{
                System.out.println("Invalid command!");
            }
        }

        session.close();
        System.out.println("Bye bye!");
    }
}
