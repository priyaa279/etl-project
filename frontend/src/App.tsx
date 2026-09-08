import { lazy, Suspense } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import { LoadingState } from "./components/PageState";
import { AppShell } from "./layouts/AppShell";

const OverviewPage = lazy(() =>
  import("./pages/OverviewPage").then((module) => ({ default: module.OverviewPage })),
);
const DatasetsPage = lazy(() =>
  import("./pages/DatasetsPage").then((module) => ({ default: module.DatasetsPage })),
);
const DatasetDetailPage = lazy(() =>
  import("./pages/DatasetDetailPage").then((module) => ({ default: module.DatasetDetailPage })),
);
const RunsPage = lazy(() =>
  import("./pages/RunsPage").then((module) => ({ default: module.RunsPage })),
);
const RunDetailPage = lazy(() =>
  import("./pages/RunDetailPage").then((module) => ({ default: module.RunDetailPage })),
);
const QualityPage = lazy(() =>
  import("./pages/QualityPage").then((module) => ({ default: module.QualityPage })),
);
const SchemaDriftPage = lazy(() =>
  import("./pages/SchemaDriftPage").then((module) => ({ default: module.SchemaDriftPage })),
);
const WatermarksPage = lazy(() =>
  import("./pages/WatermarksPage").then((module) => ({ default: module.WatermarksPage })),
);
const UploadPage = lazy(() =>
  import("./pages/UploadPage").then((module) => ({ default: module.UploadPage })),
);
const OnboardingNewPage = lazy(() =>
  import("./pages/OnboardingPage").then((module) => ({ default: module.OnboardingNewPage })),
);
const OnboardingSessionPage = lazy(() =>
  import("./pages/OnboardingPage").then((module) => ({ default: module.OnboardingSessionPage })),
);
const ReviewCenterPage = lazy(() =>
  import("./pages/ReviewCenterPage").then((module) => ({ default: module.ReviewCenterPage })),
);

export function App() {
  return (
    <Suspense fallback={<div className="p-8"><LoadingState label="Loading view" /></div>}>
      <Routes>
        <Route element={<AppShell />}>
          <Route index element={<OverviewPage />} />
          <Route path="datasets" element={<DatasetsPage />} />
          <Route path="datasets/:dataset" element={<DatasetDetailPage />} />
          <Route path="datasets/:dataset/upload" element={<UploadPage />} />
          <Route path="onboarding/new" element={<OnboardingNewPage />} />
          <Route path="onboarding/:onboardingId" element={<OnboardingSessionPage />} />
          <Route path="onboarding/:onboardingId/review" element={<ReviewCenterPage />} />
          <Route path="runs" element={<RunsPage />} />
          <Route path="runs/:runId" element={<RunDetailPage />} />
          <Route path="quality" element={<QualityPage />} />
          <Route path="schema-drift" element={<SchemaDriftPage />} />
          <Route path="watermarks" element={<WatermarksPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </Suspense>
  );
}
