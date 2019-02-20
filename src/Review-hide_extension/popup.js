// Copyright 2018 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

'use strict';

$(document).ready(function() {
  // First, functionality to hide sliders
  // document.getElementById("averageReview").addEventListener("input", hideSliders); // This kludge because I couldn't get oninput to work.
  // var slideContainer = document.getElementById("slidecontainer");
  var minSlider       = $("#minSlider");
  var minOutput       = $("#curMinVal");
  var maxSlider       = $("#maxSlider");
  var maxOutput       = $("#curMaxVal");
  var wordSlider      = $("#wordSlider");
  var wordCount       = $("#wordCount");
  var lowReviewCheck  = $("#lowReviewCheck");
  var highReviewcheck = $("#highReviewcheck");
  var wordCountCheck  = $("#wordCountCheck");

  minOutput.innerHTML = minSlider.value; // Display the default slider value
  maxOutput.innerHTML = maxSlider.value;
  wordCount.innerHTML = wordSlider.value;

  // $('.slider').slider({ disabled: true });

  // wordCountCheck.oninput = function() {
  //     wordSlider.toggle();
  // }



  // Now updated the slider values
  // First with min value
  $("#minSlider").on('input', function() {
    // Show min value in display
    $("#curMinVal").html($(this).val());
    sendToContent('#curMinVal', $(this).val())
  });

  // Same thing, now with max value
  $("#maxSlider").on('input', function() {
    // Show min value in display
    $("#curMaxVal").html($(this).val());
    sendToContent('#curMaxVal', $(this).val())
  });


  // Finally word slider
  $("#wordSlider").on('input', function() {
    // Show min value in display
    $("#wordCount").html($(this).val());
    sendToContent('#wordCount', $(this).val())
  });


  //   chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
  //   chrome.tabs.sendMessage(tabs[0].id, {greeting: "hello"}, function(response) {
  //     console.log(response.farewell);
  //   });
  // });

});

function sendToContent(which, val) {
    // Send to content.js. Note that which is a jQuery selector.
    chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
      chrome.tabs.sendMessage(tabs[0].id, {selector: which, val: val}, function(response) {
        console.log(response.farewell);
      });
    });
  }
