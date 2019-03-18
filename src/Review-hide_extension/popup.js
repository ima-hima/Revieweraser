'use strict';

$(document).ready(function() {
  // First, functionality to hide sliders
  // document.getElementById("averageReview").addEventListener("input", hideSliders); // This kludge because I couldn't get oninput to work.
  var minSlider       = $("#minSlider");
  var minOutput       = $("#curMinVal");
  var maxSlider       = $("#maxSlider");
  var maxOutput       = $("#curMaxVal");
  var wordSlider      = $("#wordSlider");
  var wordCount       = $("#wordCount");
  var lowReviewCheck  = $("#lowReviewCheck");
  var highReviewCheck = $("#highReviewCheck");
  var wordCountCheck  = $("#wordCountCheck");

  minOutput.innerHTML = minSlider.value; // Display the default slider value
  maxOutput.innerHTML = maxSlider.value;
  wordCount.innerHTML = wordSlider.value;

  // ***IMPORTANT***
  // In all of following, if the checkbox is checked, the value is true, so true == hide
  // Check for button selection
  $("#lowReviewCheck").on('input', function() {
    sendToContent('lowReviewCheck', $(this).is(':checked'), minSlider.val());
  });

  $("#highReviewCheck").on('input', function() {
    sendToContent('highReviewCheck', $(this).is(':checked'), maxSlider.val());
  });

  $("#wordCountCheck").on('input', function() {
    sendToContent('wordCountCheck', $(this).is(':checked'), wordSlider.val());
  });


  // Now updated the slider values
  // First with min value
  $("#minSlider").on('input', function() {
    // Show min value in display
    $("#curMinVal").html($(this).val());
    sendToContent('curMinVal', lowReviewCheck.is(':checked'), $(this).val());
  });

  // Same thing, now with max value
  $("#maxSlider").on('input', function() {
    // Show max value in display
    $("#curMaxVal").html($(this).val());
    sendToContent('curMaxVal', highReviewCheck.is(':checked'), $(this).val());
  });


  // Finally word slider
  $("#wordSlider").on('input', function() {
    // Show min value in display
    $("#wordCount").html($(this).val());
    sendToContent('wordCount', wordCountCheck.is(':checked'), $(this).val());
  });

});

// Send to content.js. Note that `which_selector` is a jQuery selector name.
// `checkbox`: Boolean describing whether the relevant check box is checked.
// `sliderVal`: Float of val that relevant slider is set to.
function sendToContent(which_selector, checkbox, sliderVal) {
  chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
    chrome.tabs.sendMessage( tabs[0].id,
                             { which_selector: which_selector, checkbox: checkbox, sliderVal: sliderVal },
                             function(response) {
      console.log(response.farewell);
    });
  });
}
