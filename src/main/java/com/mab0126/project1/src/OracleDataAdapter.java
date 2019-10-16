package com.mab0126.project1.src;

public class OracleDataAdapter implements IDataAdapter {
    public int connect(String dbfile) {
        //...
        return CONNECTION_OPEN_OK;
    }

    public int disconnect() {
        // ...
        return CONNECTION_CLOSE_OK;

    }
    public CustomerModel loadCustomer(int id) {
        return null;
    }

    @Override
    public int saveCustomer(CustomerModel model) {
        return 0;
    }

    @Override
    public PurchaseModel loadPurchase(int id) {
        return null;
    }

    public ProductModel loadProduct(int id) {
        return null;
    }
    public int saveProduct(ProductModel model) {
        return PRODUCT_SAVED_OK;
    }

    @Override
    public int savePurchase(PurchaseModel model) {
        return 0;
    }

}
