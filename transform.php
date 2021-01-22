<?php
function customer()
{
    try {
        $server = "192.168.56.102";
        $dbuser = "dbuser";
        $db = "salesdb";
        $sqlverbindung = new mysqli($server, $dbuser, $dbuser, $db) or die("Connect failed: %s\n" . $sqlverbindung->error);
        $mng = new MongoDB\Driver\Manager("mongodb://localhost:27017");
        $bulk = new MongoDB\Driver\BulkWrite;
        $sql = "select * from customers;";
        if ($result = mysqli_query($sqlverbindung, $sql)) {
            while ($row = $result->fetch_assoc()) {
                $doc = ['_id' => new MongoDB\BSON\ObjectID, 'customerNumber' => $row['customerNumber'],
                    'customerName' => $row['customerName'], 'contactFirstName' => $row['contactFirstName'],
                    'contactLastName' => $row['contactLastName'], 'phone' => $row['phone'],
                    'adressLine1' => $row['addressLine1'], 'adressLine2' => $row['addressLine2'],
                    'city' => $row['city'],'state' => $row['state'],'postalCode' => $row['postalCode'],
                    'country' => $row['country'],'salesRepEmployeeNumber' => $row['salesRepEmployeeNumber'],
                    'creditLimit' => $row['creditLimit']];
                $bulk->insert($doc);
            }
        }
        echo "Kundendaten wurden erfolgreich Importiert!<br>";
        $mng->executeBulkWrite('transform.customer', $bulk);
    } catch (MongoDB\Driver\Exception\Exception $e) {
        echo "Bei dem Import der Kundendaten ist folgender Fehler aufgetretten:<br>", $e->getMessage(), "\n";
    }
}
function products()
{
    try {
        $server = "192.168.56.102";
        $dbuser = "dbuser";
        $db = "salesdb";
        $sqlverbindung = new mysqli($server, $dbuser, $dbuser, $db) or die("Connect failed: %s\n" . $sqlverbindung->error);
        $mng = new MongoDB\Driver\Manager("mongodb://localhost:27017");
        $bulk = new MongoDB\Driver\BulkWrite;
        $sql = "select * from products join productlines on products.productLine = productlines.productLine;";
        if ($result = mysqli_query($sqlverbindung, $sql)) {
            while ($row = $result->fetch_assoc()) {
                $doc = ['_id' => new MongoDB\BSON\ObjectID, 'productCode' => $row['productCode'],
                    'productName' => $row['productName'], 'productScale' => $row['productScale'],
                    'productVendor' => $row['productVendor'], 'productDescription' => $row['productDescription'],
                    'quantityInStock' => $row['quantityInStock'], 'buyPrice' => $row['buyPrice'],
                    'MSRP' => $row['MSRP'],'productLine' => $row['productLine'],
                    'textDescription' => $row['textDescription'], 'htmlDescription' => $row['htmlDescription'],
                    'image' => $row['image']];
                $bulk->insert($doc);
            }
        }
        echo "Produktdaten wurden erfolgreich Importiert!<br>";
        $mng->executeBulkWrite('transform.products', $bulk);
    } catch (MongoDB\Driver\Exception\Exception $e) {
        echo "Bei dem Import der Produktdaten ist folgender Fehler aufgetretten:<br>", $e->getMessage(), "\n";
    }
}
function orders()
{
    try {
        $server = "192.168.56.102";
        $dbuser = "dbuser";
        $db = "salesdb";
        $sqlverbindung = new mysqli($server, $dbuser, $dbuser, $db) or die("Connect failed: %s\n" . $sqlverbindung->error);
        $mng = new MongoDB\Driver\Manager("mongodb://localhost:27017");
        $bulk = new MongoDB\Driver\BulkWrite;
        $sql = "select * from orders join customers on customers.customerNumber = orders.customerNumber;";
        if ($result = mysqli_query($sqlverbindung, $sql)) {
            while ($row = $result->fetch_assoc()) {
                $doc = ['_id' => new MongoDB\BSON\ObjectID, 'orderNumber' => $row['orderNumber'],
                    'orderDate' => $row['orderDate'], 'requiredDate' => $row['requiredDate'],
                    'shippedDate' => $row['shippedDate'], 'status' => $row['status'],
                    'comments' => $row['comments'], 'customerNumber' => $row['customerNumber'],
                    'customerName' => $row['customerName'],'addressLine1' => $row['addressLine1'],
                    'addressLine2' => $row['addressLine2'], 'city' => $row['city'],
                    'state' => $row['state'], 'postalCode' => $row['postalCode'], 'country' => $row['country']];
                $bulk->insert($doc);
            }
        }
        echo "Bestellungsdaten wurden erfolgreich Importiert!<br>";
        $mng->executeBulkWrite('transform.orders', $bulk);
    } catch (MongoDB\Driver\Exception\Exception $e) {
        echo "Bei dem Import der Bestellungsdaten ist folgender Fehler aufgetretten:<br>", $e->getMessage(), "\n";
    }
}
function employees()
{
    try {
        $server = "192.168.56.102";
        $dbuser = "dbuser";
        $db = "salesdb";
        $sqlverbindung = new mysqli($server, $dbuser, $dbuser, $db) or die("Connect failed: %s\n" . $sqlverbindung->error);
        $mng = new MongoDB\Driver\Manager("mongodb://localhost:27017");
        $bulk = new MongoDB\Driver\BulkWrite;
        $sql = "select * from employees join offices on employees.officeCode = offices.officeCode;";
        if ($result = mysqli_query($sqlverbindung, $sql)) {
            while ($row = $result->fetch_assoc()) {
                $innerdoc = ['supervisor' => $row['reportsTo'],
                    'jobTitle' => $row['jobTitle'],'officeCode' => $row['officeCode'],
                    'city' => $row['city'], 'phone' => $row['phone'],
                    'addressLine1' => $row['addressLine1'], 'addressLine2' => $row['addressLine2'],
                    'state' => $row['state'], 'country' => $row['country'],'postalCode' => $row['postalCode'],
                    'territory' => $row['territory']];
                $doc = ['_id' => new MongoDB\BSON\ObjectID, 'employeeNumber' => $row['employeeNumber'],
                    'lastName' => $row['lastName'], 'firstName' => $row['firstName'],
                    'extension' => $row['extension'], 'email' => $row['email'],'supervisor' => $innerdoc,
                    'territory' => $row['territory']];
                $bulk->insert($doc);
            }
        }
        echo "Mitarbeiterdaten wurden erfolgreich Importiert!<br>";
        $mng->executeBulkWrite('transform.employees', $bulk);
    } catch (MongoDB\Driver\Exception\Exception $e) {
        echo "Bei dem Import der Mitarbeiterdaten ist folgender Fehler aufgetretten:<br>", $e->getMessage(), "\n";
    }
}
customer();
products();
orders();
employees();
?>