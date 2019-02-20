'use strict';


// on load:
  //    user_ids = gather all user ids and ping server.
  //    for each of user_ids:
  //        if number of reviews > 10:
  //            compute averages, put in relevants_arr
  //            add fields for each of three criteria to relevants_arr


// on update:
//    we're turning on criterium_a:
//        log which criterium to be updated
//        for each of relevants_arr:
//            for each of relevants_arr
//            if match criterium_a:
//                hide user
//            update user[criterium_a] to true
//    else we're turning off criterium_a:
//        for each of relevants_arr:
//            if user[criterium_a] is true and others are false:
//                show user
//            user[criterium_a] = false


$(document).ready(function() {

  var relevants_arr = {}; // This will be list of users who have > 10 reviews, i.e. relevant users.


  // for each input for listener:
       // I'm just going to assume that people with high average reviews aren't
       // the same as those with low ones, and do just two conditionals. I.e. I'm not
       // going to unhide/hide depending on multiple criteria, just update depending on
       // The most recent input.
  //   if clicked off then step through and turn back on

  chrome.runtime.onMessage.addListener(
    function(request, sender, sendResponse) {
      // console.log(request);
      // console.log(sender.tab ?
      //             "from a content script:" + sender.tab.url :
      //             "from the extension");
      // alert(request.selector + ' ' + request.val);
      update(request.which_selector, request.checkbox, request.sliderVal);
      sendResponse({farewell: "nope"});
      return true;
    }
  );

  function selector_update(which_selector, checkbox, sliderVal) {
    console.log('selector_update', which_selector, checkbox, sliderVal)
    // step through relevants_arr
      // We have to check both that slider is active and that it has correct value.
      // We can't just set to active or not, because of else clause in `update()`.
      // If checkbox is off, it's off. If it's on we also need to check the slider value.
    $.each(relevants_arr, function(index, object) {
      if (which_selector == 'wordCountCheck') {
        object['wordCountCheck'] = (checkbox && sliderVal > object['avgWords']);
      } else if (which_selector == 'lowReviewCheck') {
        object['lowReviewCheck'] = (checkbox && sliderVal > object['avgStars']);
      } else {
        object['highReviewCheck'] = (checkbox && sliderVal < object['avgStars']);
      }
      console.log(object['wordCountCheck'], object['lowReviewCheck'], object['highReviewCheck']);
    });
  }

  // For each of the relevants_arr, if the criterium of the which selector matches, hide it.
  // Remember that I need to check for true/false values for all three selector values.
  // which_selector possibilities:
    // lowReviewCheck
    // highReviewCheck
    // wordCountCheck
    // curMinVal
    // curMaxVal
    // wordCount

  /*************** In this fn, remember that a value of true means to hide something. ***************/
  function update(which_selector, checkbox, sliderVal) {
    console.log('update', which_selector, checkbox, sliderVal)
    // if it's a checkbox
    if (which_selector == 'wordCountCheck' ||
        which_selector == 'lowReviewCheck' ||
        which_selector == 'highReviewCheck'
       ) {
      selector_update(which_selector, checkbox, sliderVal);
    } else {
      // At this point the checkbox is either on or off. If the checkbox is what changed ignore all this.
      // step through relevants_arr
          // if relevant value matches and the checkbox is checked
            // set checkbox to true
          // else (no longer a match)
            // set checkbox to false
      // step through a.a-profile
        // if element has any true in relevants_arr
          // hide it
        // else
          // show it
      $.each(relevants_arr, function(index, object) {
        if (which_selector == 'wordCount') {
          if (object['avgWords'] <= sliderVal && checkbox) {
            object['wordCountCheck'] = true;
          } else {
            object['wordCountCheck'] = false;
          }
        } else if (which_selector == 'curMinVal') {
          if (object['avgStars'] <= sliderVal && checkbox) {
            object['lowReviewCheck'] = true;
          } else {
            object['lowReviewCheck'] = false;
          }
        } else { // must be maxValue
          if (object['avgStars'] > sliderVal && checkbox) {
            object['highReviewCheck'] = true;
          } else {
            object['highReviewCheck'] = false;
          }
        }
      });
    }
    $('.a-profile').each(function() {
      var user_id = get_user_id( $(this).attr('href') );
      if (user_id in relevants_arr) {
        var object = relevants_arr[user_id];
        // true == hide
        if (object['lowReviewCheck'] || object['highReviewCheck'] || object['wordCountCheck']) {
          $(this).parent().parent().css("display", "none");
        } else {
          $(this).parent().parent().css("display", "contents");
        }
      console.log(object['wordCountCheck'], object['lowReviewCheck'], object['highReviewCheck']);
      }
    });
  }
  function get_user_id(input_url) {
    var secondidx  = input_url.search('/ref'); // second index of the substring containing the user id
    return input_url.substring(26, secondidx); // it always starts at 25
  }


  // Create a url with GET query string for pinging web server
  function get_url() {
    var query_string = '?';
    var idx = 0;
    $( "a.a-profile" ).each(function() {
      query_string += ('id' + idx + '=' + get_user_id( $(this).attr('href') ) + '&');
      idx++;
    });
    return ('https://storystreetconsulting.com/wsgi' + query_string);
  }

  var get_string = get_url(); // Need this to send data to ajax url, because it won't evaluate a fn.

  // send url to server, get response, save it into array of relevant users
  $.ajax({
    dataType: 'json',
    url: get_string,
    success: function(return_data) {
      $.each(return_data, function(index, value) {
        if (value['num'] >= 10) {  // relevant users have at least 10 reviews
          relevants_arr[index] = {  // these divisions should be safe, because we know there are reviews
                                    'avgStars':        value['stars'] / value['num'],
                                    'avgWords':        value['words'] / value['num'],
                                    'lowReviewCheck':  false,
                                    'highReviewCheck': false,
                                    'wordCountCheck':  false
                                   };
        }
      });

    },
    error: function(xhr, status, errorMsg) {
      $("#results").append("error");
      console.log(errorMsg);
    }
  });


});

