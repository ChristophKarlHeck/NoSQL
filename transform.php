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
                $sql2 = "select * from payments where ";
                echo "Abgewickelt:". $row['customerNumber']. "<br>";
                $doc = ['_id' => new MongoDB\BSON\ObjectID, 'customerNumber' => $row['customerNumber'],
                    'customerName' => $row['customerName'], 'contactFirstName' => $row['contactFirstName'],
                    'contactLastName' => $row['contactLastName'], 'phone' => $row['phone'],
                    'adressLine1' => $row['addressLine1'], 'adressLine2' => $row['addressLine2'],
                    'city' => $row['city'],'state' => $row['state'],'postalCode' => $row['postalCode']];
                $bulk->insert($doc);
            }
        }
        $mng->executeBulkWrite('transform.customer', $bulk);
    } catch (MongoDB\Driver\Exception\Exception $e) {
        echo "Fehler:", $e->getMessage(), "\n";
    }
}
customer();
?>