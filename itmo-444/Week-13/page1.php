<?php
// page1.php

session_start();

echo 'Welcome to page #1';

$_SESSION['favcolor'] = 'green';    
$_SESSION['animal']   = 'cat';
$_SESSION['time']     = time();


// Works if session cookie was accepted
echo '<br /><a href="page2.php">page 2</a><br />';
# sample here on css visibility and display
#https://stackoverflow.com/questions/133051/what-is-the-difference-between-visibilityhidden-and-displaynone

if (!isset($_SESSION['myanimal'])) {
    echo "Not yet declared!\n";
  } else {
      echo "Value is set.\n";
  }
 if ($_SESSION['myanimal'] == 'Cat') {
    echo '<span><img src="cat.jpg" /></span>';
  }  else {
    echo '<span style="visibility:hidden"><img src="cat.jpg" /></span>';
  }  

?> 


