function initializeJS() {
  jQuery('.driverPicker').selectpicker();
  jQuery('.driverPicker').change(toggleDownload);
  jQuery('.releasePicker').selectpicker();
  jQuery('.releasePicker').change(toggleDownload);
  jQuery('.distroPicker').bootstrapToggle();
  jQuery('.distroPicker').change(toggleDownload);

  var clipboard = new ZeroClipboard(jQuery(".clipboard button"));
  clipboard.on( 'copy', function(event) {
    var button = jQuery(".clipboard button");
    button.addClass('btn-success');
    clipboard.clearData();
    prefix = $('.distroPicker').prop('checked') ? "#maven" : "#gradle"
    driverVersion = $('.driverPicker').selectpicker().val();
    releaseVersion = $('.releasePicker').selectpicker().val();
    activeSample = prefix + "-" + releaseVersion + "-" + driverVersion;
    clipboard.setText($(activeSample).text());

    button.animate({ opacity: 1 }, 300, function() {
      button.removeClass('btn-success');
    });
  });
};

var toggleDownload = function() {
  downloadLink = 'https://oss.sonatype.org/content/repositories/releases/org/mongodb/';
  prefix = $('.distroPicker').prop('checked') ? "#maven" : "#gradle"
  driverVersion = $('.driverPicker').selectpicker().val();
  releaseVersion = $('.releasePicker').selectpicker().val();
  activeSample = prefix + "-" + releaseVersion + "-" + driverVersion;
  activeDescription = "#driver-" + driverVersion;

  activeDriver = $('.driverPicker option:selected').text();
  activeVersion = $('.releasePicker option:selected').text();
  activeLink = downloadLink + activeDriver +'/' + activeVersion + '/';

  $('.download').addClass('hidden');
  $(activeSample).removeClass('hidden');
  $(activeDescription).removeClass('hidden');
  $('#downloadLink').attr('href', activeLink);
};

jQuery(document).ready(function(){
  initializeJS();
  jQuery("body").addClass("hljsCode");
  hljs.initHighlightingOnLoad();
});
