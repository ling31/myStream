package com.ling.kafka.stream.sql.ignite;


/**
 * Ignite单例模式.
 * 
 * @author smartloli.
 *
 *         Created by Jun 20, 2018
 */
final class IgniteRepositoryFactory {

    private static IgniteRepository instance;

    static synchronized IgniteRepository getInstance() {
        if (instance == null) {
            instance = new IgniteRepository();
            instance.init();
        }
        return instance;
    }

}
