package com.mab0126.project1.src;

import javax.swing.*;
import java.awt.*;

public class AddProductView extends JFrame {

    public JButton btnAdd = new JButton("Add");
    public JButton btnCancel = new JButton("Cancel");

    public JTextField txtProductID = new JTextField(20);
    public JTextField txtName = new JTextField(20);
    public JTextField txtPrice = new JTextField(20);
    public JTextField txtQuantity = new JTextField(20);


    public AddProductView() {
        this.setTitle("Add Product");
        this.setSize(600, 400);
        this.getContentPane().setLayout(new BoxLayout(this.getContentPane(), BoxLayout.PAGE_AXIS));

        String[] labels = {"ProductID ", "Name ", "Price ", "Quantity "};

        JPanel line1 = new JPanel(new FlowLayout());
        line1.add(new JLabel("ProductID "));
        line1.add(txtProductID);
        this.getContentPane().add(line1);

        JPanel line2 = new JPanel(new FlowLayout());
        line2.add(new JLabel("Name "));
        line2.add(txtName);
        this.getContentPane().add(line2);

        JPanel line3 = new JPanel(new FlowLayout());
        line3.add(new JLabel("Price "));
        line3.add(txtPrice);
        this.getContentPane().add(line3);

        JPanel line4 = new JPanel(new FlowLayout());
        line4.add(new JLabel("Quantity "));
        line4.add(txtQuantity);
        this.getContentPane().add(line4);

        JPanel panelButtons = new JPanel(new FlowLayout());
        panelButtons.add(btnAdd);
        panelButtons.add(btnCancel);
        this.getContentPane().add(panelButtons);

//Lay out the panel.
//        SpringUtilities.makeCompactGrid(p,
//                numPairs, 2, //rows, cols
//                6, 6,        //initX, initY
//                6, 6);       //xPad, yPad

    }

}
