package com.github.linyuzai.shardingjdbc.schedule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.util.List;

public class DynamicTableCreatorRunner implements ApplicationRunner {

    @Autowired(required = false)
    private List<DynamicTableCreator> creators;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (creators == null) {
            return;
        }
        for (DynamicTableCreator creator : creators) {
            throwIfException(creator.createTable(0, 1));
        }
    }

    private void throwIfException(Exception e) throws Exception {
        if (e != null) {
            throw e;
        }
    }
}
