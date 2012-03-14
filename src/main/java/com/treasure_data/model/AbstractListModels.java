package com.treasure_data.model;

import java.util.List;

abstract class AbstractListModels<T extends AbstractModel>
        extends AbstractModel implements ListModels<T> {

    private List<T> list;

    protected AbstractListModels(List<T> list) {
        super();
        this.list = list;
    }

    /**
     * @see com.treasure_data.data.model.AbstractModel#getName()
     */
    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    public List<T> get() {
        return list;
    }

    public T get(String name) {
        for (T t : list) {
            if (t.getName().equals(name)) {
                return t;
            }
        }
        return null;
    }

    public boolean delete(String name) {
        int j = -1;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getName().equals(name)) {
                j = i;
                break;
            }
        }

        if (j != -1) {
            list.remove(j);
            return true;
        } else {
            return false;
        }
    }

    public int size() {
        return list.size();
    }
}
